#!/bin/bash

# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

set -e

err () {
    echo "ERROR: $1" >&2
    exit 1
}

if [ -z "$E2E_TEST_TOOLS_DIR" ]; then
  err "Please set E2E_TEST_TOOLS_DIR to the root directory of the spark-rapids-tools repository. Exiting script."
fi

if [ -z "$E2E_TEST_SPARK_BUILD_VERSION" ]; then
  err "Please set E2E_TEST_SPARK_BUILD_VERSION to the version of Spark used for building Tools JAR. Exiting script."
fi

if [ -z "$E2E_TEST_HADOOP_VERSION" ]; then
  err "Please set E2E_TEST_HADOOP_VERSION to the version of Hadoop used for building Tools JAR. Exiting script."
fi

build_wheel() {
  local python_tools_dir="$E2E_TEST_TOOLS_DIR/user_tools"
  echo "Building Spark RAPIDS Tools wheel using build.sh in non-fat mode"
  pushd "$python_tools_dir"

  # Construct Maven arguments from environment variables
  local maven_args=""

  # Add buildver parameter if provided
  if [ -n "$E2E_TEST_SPARK_BUILD_VERSION" ]; then
    maven_args="$maven_args -Dbuildver=$E2E_TEST_SPARK_BUILD_VERSION"
    echo "Using buildver: $E2E_TEST_SPARK_BUILD_VERSION"
  fi

  # Add hadoop.version parameter if provided
  if [ -n "$E2E_TEST_HADOOP_VERSION" ]; then
    maven_args="$maven_args -Dhadoop.version=$E2E_TEST_HADOOP_VERSION"
    echo "Using hadoop.version: $E2E_TEST_HADOOP_VERSION"
  fi

  # Set TOOLS_MAVEN_ARGS environment variable for build.sh
  if [ -n "$maven_args" ]; then
    export TOOLS_MAVEN_ARGS="$maven_args"
    echo "Maven arguments: $TOOLS_MAVEN_ARGS"
  fi

  echo "Running: ./build.sh non-fat"
  ./build.sh non-fat

  if [ $? -ne 0 ]; then
    echo "Failed to build the wheel file"
    exit 1
  fi

  popd
}

install_python_package() {
  if [ -z "$E2E_TEST_VENV_DIR" ]; then
    err "Please set E2E_TEST_VENV_DIR to the name of the virtual environment. Exiting script."
  fi

  echo "Setting up Python environment in $E2E_TEST_VENV_DIR"
  local python_tools_dir="$E2E_TEST_TOOLS_DIR/user_tools"
  python -m venv "$E2E_TEST_VENV_DIR"
  source "$E2E_TEST_VENV_DIR"/bin/activate

  echo "Installing Spark RAPIDS Tools Python package from wheel"
  pushd "$python_tools_dir"
  pip install --upgrade pip setuptools wheel

  # Find and install the wheel file
  local wheel_file=$(find dist/ -name "*.whl" -type f | head -n 1)
  if [ -z "$wheel_file" ]; then
    echo "No wheel file found in dist/ directory"
    exit 1
  fi

  echo "Installing wheel: $wheel_file"
  pip install "$wheel_file"
  popd
}

# Check if we need to build the wheel file
python_tools_dir="$E2E_TEST_TOOLS_DIR/user_tools"
wheel_exists=false
if [ -d "$python_tools_dir/dist" ] && [ -n "$(find "$python_tools_dir/dist" -name "*.whl" -type f 2>/dev/null)" ]; then
  wheel_exists=true
fi

# Build wheel if: wheel doesn't exist AND E2E_BUILD_WHEEL is explicitly true
if [ "$wheel_exists" = false ] && [ "$E2E_TEST_BUILD_WHEEL" = "true" ]; then
  build_wheel
fi

install_python_package
