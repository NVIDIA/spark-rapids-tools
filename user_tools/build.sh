#!/bin/bash

# This script takes a build mode ("offline" or otherwise) as a parameter.
# If the build mode is "offline", it runs the dependency downloader script.
# If the build mode is not "offline", it removes dependencies from the offline directory
# Finally performs the default build process.

# Function to run the dependency downloader script for offline mode
download_offline_dependencies() {
  local resource_dir="$1"
  python "$resource_dir/offline_downloader.py" "$resource_dir"
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

build_mode="$1"
resource_dir="src/spark_rapids_pytools/resources"

if [ "$build_mode" = "offline" ]; then
  echo "Building in offline mode"
  download_offline_dependencies "$resource_dir"
else
  remove_offline_dependencies "$resource_dir"
fi

python -m build
