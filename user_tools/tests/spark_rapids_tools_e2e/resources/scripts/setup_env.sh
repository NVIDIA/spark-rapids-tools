#!/bin/bash

# Copyright (c) 2024, NVIDIA CORPORATION.
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

build_jar() {
  local jar_tools_dir="$E2E_TEST_TOOLS_DIR/core"
  echo "Building Spark RAPIDS Tools JAR file"
  pushd "$jar_tools_dir"
  mvn install -DskipTests -Dbuildver="$E2E_TEST_SPARK_BUILD_VERSION" -Dhadoop.version="$E2E_TEST_HADOOP_VERSION"
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

  echo "Installing Spark RAPIDS Tools Python package"
  pushd "$python_tools_dir"
  pip install --upgrade pip setuptools wheel
  pip install .
  popd
}

# Check if the Tools JAR file exists or if the user wants to build it
if [ ! -f "$E2E_TEST_TOOLS_JAR_PATH" ] || [ "$E2E_TEST_BUILD_JAR" = "true" ]; then
  build_jar
fi

install_python_package
