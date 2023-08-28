#!/bin/bash
# Copyright (c) 2023, NVIDIA CORPORATION.
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
PREPACKAGED_FOLDER="csp-resources"

# Constants and variables of core module
CORE_DIR="$WORK_DIR/../core"
TOOLS_JAR_FILE=""


# Function to run mvn command to build the tools jar
build_jar_from_source() {
  # store teh current directory
  local curr_dir=$(pwd)
  local jar_dir=$CORE_DIR/target
  cd "$CORE_DIR"
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
  cd "$curr_dir"
}

# Function to run the dependency downloader script for offline mode
download_offline_dependencies() {
  local res_dir="$1"
  local offline_downloader_script="$res_dir/dev/prepackage_mgr.py"
  python "$offline_downloader_script" run --resource_dir="$res_dir" --tools_jar="$TOOLS_JAR_FILE"
  if [ $? -ne 0 ]; then
    echo "Dependency download failed for offline mode. Exiting"
    exit 1
  fi
}

# Function to remove dependencies from the offline directory
remove_offline_dependencies() {
  local res_dir="$1"
  # remove folder recursively
  rm -rf "$res_dir/$PREPACKAGED_FOLDER"
  # remove compressed file in case archive-mode was enabled
  rm "$res_dir/$PREPACKAGED_FOLDER.tgz"
}

# Pre-build setup
pre_build() {
  rm -rf build/ dist/
  pip install build -e .
}

# Build process
build() {
  remove_offline_dependencies "$RESOURCE_DIR"
  if [ "$build_mode" = "fat" ]; then
    echo "Building in fat mode"
    build_jar_from_source
    download_offline_dependencies "$RESOURCE_DIR"
  fi
  python -m build --wheel
}

# Main script execution
pre_build
build "$build_mode"

# Check build status
if [ $? -eq 0 ]; then
  echo "Build successful. To install, use: pip install <wheel-file>"
else
  echo "Build failed."
  exit 1
fi
