#!/bin/bash
# Copyright (c) 2023-2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Usage: ./build.sh [build_mode] [jar_url]
# This script takes a build_mode ("fat" or "non-fat") and the jar url as parameter.
# Build mode is a mandatory parameter while the jar url is optional.
# Additional Maven arguments can be passed via the TOOLS_MAVEN_ARGS environment variable.
# Example: TOOLS_MAVEN_ARGS="-Dbuildver=350 -Dhadoop.version=3.3.6" ./build.sh non-fat
# If the build_mode is "fat" and jar_url is provided, it downloads the JAR from the URL and packages the CSP dependencies with the whl.
# If the build_mode is "fat" and jar_url is not provided, it builds the JAR from source and packages the CSP dependencies with the whl.
# If the build_mode is "non-fat" and jar_url is provided, it downloads the JAR from the URL and packages it with the wheel.
# If the build_mode is "non-fat" and jar_url is not provided, it builds the JAR from source and packages it with the wheel.

# Get the build mode argument
# Check if build_mode is provided and valid
if [ -z "$1" ]; then
  echo "Error: build_mode parameter is required. Use either 'fat' or 'non-fat'."
  exit 1
fi

# Validate build_mode is either "fat" or "non-fat"
if [ "$1" != "fat" ] && [ "$1" != "non-fat" ]; then
  echo "Error: build_mode must be either 'fat' or 'non-fat'. Got '$1' instead."
  exit 1
fi

build_mode="$1"
jar_url="$2"

# get the directory of the script
WORK_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

# Define resource directory
RESOURCE_DIR="src/spark_rapids_pytools/resources"
TOOLS_RESOURCE_FOLDER="tools-resources"
PREPACKAGED_FOLDER="csp-resources"

# Constants and variables of core module
CORE_DIR="$WORK_DIR/../core"
TOOLS_JAR_FILE=""
BUILD_DIR="/var/tmp/spark_rapids_user_tools_cache/build_$(date +%Y%m%d_%H%M%S)"

# Function to download JAR from URL
download_jar_from_url() {
  local url="$1"
  local output_dir="$2"

  # Create download directory if it doesn't exist
  mkdir -p "$output_dir"

  # Extract filename from URL
  local filename=$(basename "$url")
  local output_path="$output_dir/$filename"

  # Validate the JAR filename to ensure it is not a source, javadoc, or test JAR
  if [[ "$filename" == *"sources.jar" || "$filename" == *"javadoc.jar" || "$filename" == *"tests.jar" ]]; then
    echo "Invalid JAR file: $filename. Source, javadoc, or test JARs are not allowed."
    exit 1
  fi

  echo "Downloading JAR from $url to $output_path"
  curl -L -f -o "$output_path" "$url"

  if [ $? -ne 0 ]; then
    echo "Failed to download JAR from URL: $url"
    exit 1
  fi

  TOOLS_JAR_FILE="$output_path"
  echo "Downloaded JAR file: $TOOLS_JAR_FILE"
}

clean_up_build_jars() {
  if [ -d "$BUILD_DIR" ]; then
    rm -rf "$BUILD_DIR"
  fi
}

# Function to run mvn command to build the tools jar
# This function skips the test cases and builds the jar file and only
# picks the jar file without sources/javadoc/tests..
# Maven arguments can be passed via TOOLS_MAVEN_ARGS environment variable
build_jar_from_source() {
  # store teh current directory
  local curr_dir
  curr_dir=$(pwd)
  local jar_dir="$CORE_DIR"/target
  cd "$CORE_DIR" || exit

  # Construct Maven command with optional arguments from environment
  local mvn_cmd="mvn clean package -DskipTests"

  # Add additional Maven arguments if provided via environment variable
  if [ -n "$TOOLS_MAVEN_ARGS" ]; then
    mvn_cmd="$mvn_cmd $TOOLS_MAVEN_ARGS"
    echo "Using additional Maven arguments: $TOOLS_MAVEN_ARGS"
  fi

  echo "Running Maven command: $mvn_cmd"

  # build mvn
  eval "$mvn_cmd"
  if [ $? -ne 0 ]; then
    echo "Failed to build the tools jar"
    exit 1
  fi
  # rename jar file stripping snapshot
  TOOLS_JAR_FILE=( "$( find "${jar_dir}" -type f \( -iname "rapids-4-spark-tools_*.jar" ! -iname "*sources.jar" ! -iname "*tests.jar" ! -iname "original-rapids-4*.jar" ! -iname "*javadoc.jar" \) )" )

  if [ -z "$TOOLS_JAR_FILE" ]; then
    echo "Failing because tools jar could not be located"
    exit 1
  else
    echo "Using tools jar file: $TOOLS_JAR_FILE"
  fi
  # restore the current directory
  cd "$curr_dir" || exit
}

# Function to run the dependency downloader script for non-fat/fat mode
# prepackage_mgr.py file downloads the dependencies for the csp-related resources
# in case of fat mode.
# In case of non-fat mode, it just copies the tools jar into the tools-resources folder
# --fetch_all_csp=True toggles the fat/non-fat mode for the script
download_web_dependencies() {
  local res_dir="$1"
  local is_fat_mode="$2"
  local web_downloader_script="$res_dir/dev/prepackage_mgr.py"
  echo "Downloading dependencies"
  python "$web_downloader_script" run --resource_dir="$res_dir" --tools_jar="$TOOLS_JAR_FILE" --fetch_all_csp="$is_fat_mode"
  if [ $? -ne 0 ]; then
    echo "Dependency download failed. Exiting"
    exit 1
  fi
}

# Function to remove dependencies from the fat directory
remove_web_dependencies() {
  local res_dir="$1"
  # remove tools jar
  rm -rf "${res_dir:?}"/"$TOOLS_RESOURCE_FOLDER"
  # remove folder recursively
  rm -rf "${res_dir:?}"/"$PREPACKAGED_FOLDER"
  # remove compressed file in case archive-mode was enabled
  rm -f "${res_dir:?}"/"$PREPACKAGED_FOLDER".tgz
  # remove core folder containing qualOutputTable.yaml
  rm -rf "${res_dir:?}"/core/generated_files
}

# Function to copy qualOutputTable.yaml from core module to resources/core/generated_files folder
copy_qual_output_table_yaml() {
  local res_dir="$1"
  local yaml_source="$CORE_DIR/src/main/resources/configs/qualOutputTable.yaml"
  local core_res_dir="$res_dir/core/generated_files"
  local yaml_dest="$core_res_dir/qualOutputTable.yaml"

  if [ -f "$yaml_source" ]; then
    echo "Copying qualOutputTable.yaml from core module to resources/core/generated_files"
    # Create core/generated_files directory if it doesn't exist
    mkdir -p "$core_res_dir"
    cp "$yaml_source" "$yaml_dest"
    if [ $? -ne 0 ]; then
      echo "Failed to copy qualOutputTable.yaml"
      exit 1
    fi
    echo "Successfully copied qualOutputTable.yaml to $yaml_dest"
  else
    echo "Warning: qualOutputTable.yaml not found at $yaml_source"
    exit 1
  fi
}

# Pre-build setup
pre_build() {
  echo "upgrade pip"
  pip install --upgrade pip
  echo "rm previous build and dist directories"
  rm -rf build/ dist/
  echo "install build dependencies using pip"
  pip install build -e .[qualx,test]
  # Note: Removed -e .[qualx,test] to avoid overriding existing package installations
}

# Build process
build() {
  # Deletes pre-existing csp-resources.tgz folder
  remove_web_dependencies "$RESOURCE_DIR"
  # Build the tools jar from source
  if [ -n "$jar_url" ]; then
      echo "Using provided JAR URL instead of building from source"
      download_jar_from_url "$jar_url" "$BUILD_DIR"
    else
      echo "Building JAR from source"
      build_jar_from_source
  fi
  if [ "$build_mode" = "fat" ]; then
    echo "Building in fat mode"
    # This will download the dependencies and create the csp-resources
    # and copy the dependencies into the csp-resources folder
    # Tools resources are copied into the tools-resources folder
    download_web_dependencies "$RESOURCE_DIR" "True"
  else
    echo "Building in non-fat mode"
    # This will just copy the tools jar built from source into the tools-resources folder
    download_web_dependencies "$RESOURCE_DIR" "False"
  fi
  # Copy qualOutputTable.yaml from core module
  copy_qual_output_table_yaml "$RESOURCE_DIR"
  # Builds the python wheel file
  # Look into the pyproject.toml file for the build system requirements
  python -m build --wheel
  clean_up_build_jars
}

# Main script execution
pre_build
build

# Check build status
if [ $? -eq 0 ]; then
  echo "Build successful. To install, use: pip install dist/<wheel-file>"
else
  echo "Build failed."
  exit 1
fi
