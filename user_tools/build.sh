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


# Usage: ./build.sh [build_mode]
# This script takes an optional build_mode ("fat" or otherwise) as a parameter.
# If the build_mode is "fat", it runs the dependency downloader script and build the jar from
# source.
# If the build_mode is not "fat", it removes dependencies from the prepackaged directory
# Finally performs the default build process.

# Get the build mode argument
build_mode="$1"

# get the directory of the script
WORK_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

# Define resource directory
RESOURCE_DIR="src/spark_rapids_pytools/resources"
TOOLS_RESOURCE_FOLDER="tools-resources"
PREPACKAGED_FOLDER="csp-resources"

# Constants and variables of core module
CORE_DIR="$WORK_DIR/../core"
TOOLS_JAR_FILE=""


# Function to run mvn command to build the tools jar
# This function skips the test cases and builds the jar file and only
# picks the jar file without sources/javadoc/tests..
build_jar_from_source() {
  # store teh current directory
  local curr_dir
  curr_dir=$(pwd)
  local jar_dir="$CORE_DIR"/target
  cd "$CORE_DIR" || exit
  # build mvn
  mvn clean package -DskipTests
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
  if [ "$is_fat_mode" = "true" ]; then
    echo "Downloading dependencies for fat mode"
    python "$web_downloader_script" run --resource_dir="$res_dir" --tools_jar="$TOOLS_JAR_FILE" --fetch_all_csp=True
  else
    echo "Downloading dependencies for non-fat mode"
    python "$web_downloader_script" run --resource_dir="$res_dir" --tools_jar="$TOOLS_JAR_FILE" --fetch_all_csp=False
  fi
  if [ $? -ne 0 ]; then
    echo "Dependency download failed for fat mode. Exiting"
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
  rm "${res_dir:?}"/"$PREPACKAGED_FOLDER".tgz
}

# Pre-build setup
pre_build() {
  rm -rf build/ dist/
  pip install build -e .
}

# Build process
build() {
  # Deletes pre-existing csp-resources.tgz folder
  remove_web_dependencies "$RESOURCE_DIR"
  # Build the tools jar from source
  build_jar_from_source
  if [ "$build_mode" = "fat" ]; then
    echo "Building in fat mode"
    # This will download the dependencies and create the csp-resources
    # and copy the dependencies into the csp-resources folder
    # Tools resources are copied into the tools-resources folder
    download_web_dependencies "$RESOURCE_DIR" "true"
  else
    echo "Building in non-fat mode"
    # This will just copy the tools jar built from source into the tools-resources folder
    download_web_dependencies "$RESOURCE_DIR" "false"
  fi
  # Builds the python wheel file
  # Look into the pyproject.toml file for the build system requirements
  python -m build --wheel
}

# Main script execution
pre_build
build "$build_mode"

# Check build status
if [ $? -eq 0 ]; then
  echo "Build successful. To install, use: pip install dist/<wheel-file>"
else
  echo "Build failed."
  exit 1
fi
