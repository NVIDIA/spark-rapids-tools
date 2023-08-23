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
# This script takes an optional build_mode ("offline" or otherwise) as a parameter.
# If the build_mode is "offline", it runs the dependency downloader script.
# If the build_mode is not "offline", it removes dependencies from the offline directory
# Finally performs the default build process.

# Get the build mode argument
build_mode="$1"

# Define resource directory
resource_dir="src/spark_rapids_pytools/resources"

# Function to run the dependency downloader script for offline mode
download_offline_dependencies() {
  local resource_dir="$1"
  local offline_downloader_script="$resource_dir/dev/offline_downloader.py"
  python $offline_downloader_script "$resource_dir"
  if [ $? -ne 0 ]; then
    echo "Dependency download failed for offline mode. Exiting"
    exit 1
  fi
}

# Function to remove dependencies from the offline directory
remove_offline_dependencies() {
  local resource_dir="$1"
  rm -rf "$resource_dir/offline"/*
}

# Pre-build setup
pre_build() {
  rm -rf build/ dist/
  pip install build -e .
}

# Build process
build() {
  if [ "$build_mode" = "offline" ]; then
    echo "Building in offline mode"
    download_offline_dependencies "$resource_dir"
  else
    remove_offline_dependencies "$resource_dir"
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
