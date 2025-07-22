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

  echo "=========================================="
  echo "DEBUG: Starting download_web_dependencies()"
  echo "=========================================="

  # Debug: Environment at time of prepackage_mgr.py execution
  echo "DEBUG: Environment when running prepackage_mgr.py:"
  echo "  Current directory: $(pwd)"
  echo "  which python: $(which python)"
  echo "  which python3: $(which python3)"
  echo "  python3 --version: $(python3 --version 2>&1)"
  echo "  python3 executable: $(python3 -c 'import sys; print(sys.executable)')"

  # Debug: Check sys.path for python3
  echo "DEBUG: python3 sys.path:"
  python3 -c "import sys; [print(f'  {i}: {p}') for i, p in enumerate(sys.path[:5])]"

  # Debug: Check if fire is available to python3
  echo "DEBUG: Checking if fire is available to python3:"
  python3 -c "import fire; print('fire available to python3:', fire.__version__)" 2>&1 || echo "DEBUG: fire NOT available to python3"

  # Debug: Check script existence and first few lines
  echo "DEBUG: Script path: $web_downloader_script"
  if [ -f "$web_downloader_script" ]; then
    echo "DEBUG: Script exists, checking imports:"
    head -30 "$web_downloader_script" | grep -E "^import|^from" | head -5
  else
    echo "DEBUG: ERROR - Script not found!"
  fi

  # Debug: Show the exact command about to be run
  echo "DEBUG: About to run:"
  echo "  python3 $web_downloader_script run --resource_dir=$res_dir --tools_jar=$TOOLS_JAR_FILE --fetch_all_csp=$is_fat_mode"

  echo "Downloading dependencies"

  # Run the command and capture detailed output
  python3 "$web_downloader_script" run --resource_dir="$res_dir" --tools_jar="$TOOLS_JAR_FILE" --fetch_all_csp="$is_fat_mode" 2>&1 | tee /tmp/prepackage_debug.log
  local exit_code=${PIPESTATUS[0]}

  echo "DEBUG: prepackage_mgr.py exit code: $exit_code"

  if [ $exit_code -ne 0 ]; then
    echo "DEBUG: prepackage_mgr.py FAILED, showing debug log:"
    cat /tmp/prepackage_debug.log
    echo "DEBUG: Attempting to diagnose import issue..."

    # Try to identify the specific import that's failing
    echo "DEBUG: Testing individual imports from prepackage_mgr.py:"
    python3 -c "import fire" 2>&1 || echo "DEBUG: fire import failed"
    python3 -c "import os" 2>&1 || echo "DEBUG: os import failed"
    python3 -c "import sys" 2>&1 || echo "DEBUG: sys import failed"

    # Check if this is a Python path issue
    echo "DEBUG: Current PYTHONPATH: ${PYTHONPATH:-'not set'}"
    echo "DEBUG: Trying python vs python3:"
    python -c "import fire; print('python can import fire')" 2>&1 || echo "DEBUG: python cannot import fire"
    python3 -c "import fire; print('python3 can import fire')" 2>&1 || echo "DEBUG: python3 cannot import fire"

    echo "Dependency download failed. Exiting"
    exit 1
  fi

  echo "=========================================="
  echo "DEBUG: Completed download_web_dependencies()"
  echo "=========================================="
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
  echo "=========================================="
  echo "DEBUG: Starting pre_build() function"
  echo "=========================================="

  # Debug: Environment and Python info
  echo "DEBUG: Current working directory: $(pwd)"
  echo "DEBUG: USER: $USER"
  echo "DEBUG: PATH: $PATH"
  echo "DEBUG: PYTHONPATH: ${PYTHONPATH:-'not set'}"
  echo "DEBUG: Virtual env: ${VIRTUAL_ENV:-'not set'}"

  # Debug: Python and pip locations BEFORE upgrade
  echo "DEBUG: BEFORE pip upgrade:"
  echo "  which python: $(which python)"
  echo "  which python3: $(which python3)"
  echo "  which pip: $(which pip)"
  echo "  python --version: $(python --version 2>&1)"
  echo "  pip --version: $(pip --version 2>&1)"
  echo "  python -c 'import sys; print(\"sys.executable:\", sys.executable)'"
  python -c "import sys; print('sys.executable:', sys.executable)"
  echo "  python -c 'import sys; print(\"sys.path[:3]:\", sys.path[:3])'"
  python -c "import sys; print('sys.path[:3]:', sys.path[:3])"

  echo "upgrade pip"
  pip install --upgrade pip

  # Debug: Python and pip locations AFTER upgrade
  echo "DEBUG: AFTER pip upgrade:"
  echo "  which pip: $(which pip)"
  echo "  pip --version: $(pip --version 2>&1)"
  echo "  python -m pip --version: $(python -m pip --version 2>&1)"
  echo "  python -c 'import sys; print(\"sys.path[:3]:\", sys.path[:3])'"
  python -c "import sys; print('sys.path[:3]:', sys.path[:3])"

  echo "rm previous build and dist directories"
  rm -rf build/ dist/

  echo "install build dependencies using pip"
  echo "DEBUG: Installing 'pip install build .[qualx,test]'"

  # Debug: Check if pyproject.toml exists and show dependencies
  if [ -f "pyproject.toml" ]; then
    echo "DEBUG: pyproject.toml exists, checking fire dependency:"
    grep -A 20 'dependencies = \[' pyproject.toml | head -10
  fi

  # Try the installation and capture both stdout and stderr
  echo "DEBUG: Running pip install command..."
  pip install build .[qualx,test] 2>&1 | tee /tmp/pip_install_debug.log
  pip_exit_code=${PIPESTATUS[0]}

  echo "DEBUG: pip install exit code: $pip_exit_code"

  if [ $pip_exit_code -ne 0 ]; then
    echo "DEBUG: pip install FAILED, showing debug log:"
    cat /tmp/pip_install_debug.log
  fi

  # Debug: Check if fire is available after installation
  echo "DEBUG: Checking if 'fire' module is available:"
  python -c "import fire; print('fire module found:', fire.__version__)" 2>&1 || echo "DEBUG: fire module NOT FOUND"

  # Debug: Check installed packages
  echo "DEBUG: Installed packages containing 'fire':"
  pip list | grep -i fire || echo "DEBUG: No fire package found in pip list"

  # Debug: Check site-packages location
  echo "DEBUG: Site-packages location:"
  python -c "import site; print('site-packages:', site.getsitepackages())"

  # Debug: Check if current package is installed
  echo "DEBUG: Checking current package installation:"
  pip show spark-rapids-user-tools || echo "DEBUG: spark-rapids-user-tools not found in pip list"

  echo "=========================================="
  echo "DEBUG: Completed pre_build() function"
  echo "=========================================="
  # Note: Removed -e .[qualx,test] to avoid overriding existing package installations
}

# Build process
build() {
  echo "=========================================="
  echo "DEBUG: Starting build() function"
  echo "=========================================="

  # Debug: Environment at start of build
  echo "DEBUG: Build environment:"
  echo "  Current directory: $(pwd)"
  echo "  Build mode: $build_mode"
  echo "  which python: $(which python)"
  echo "  python can import fire: $(python -c 'import fire; print("yes")' 2>/dev/null || echo "no")"

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

  # Debug: Check environment before download_web_dependencies
  echo "DEBUG: Before download_web_dependencies:"
  echo "  fire still available: $(python -c 'import fire; print("yes")' 2>/dev/null || echo "no")"

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

  # Debug: Environment before python -m build
  echo "DEBUG: Before python -m build --wheel:"
  echo "  Current directory: $(pwd)"
  echo "  fire available: $(python -c 'import fire; print("yes")' 2>/dev/null || echo "no")"
  echo "  python -m build location: $(python -c 'import build; print(build.__file__)' 2>/dev/null || echo "build module not found")"

  # Builds the python wheel file
  # Look into the pyproject.toml file for the build system requirements
  echo "DEBUG: Running python -m build --wheel"
  python -m build --wheel 2>&1 | tee /tmp/build_wheel_debug.log
  local build_exit_code=${PIPESTATUS[0]}

  echo "DEBUG: python -m build exit code: $build_exit_code"
  if [ $build_exit_code -ne 0 ]; then
    echo "DEBUG: python -m build FAILED, showing debug log:"
    cat /tmp/build_wheel_debug.log
  fi

  clean_up_build_jars

  echo "=========================================="
  echo "DEBUG: Completed build() function"
  echo "=========================================="
}

# Initial environment debugging
echo "=========================================="
echo "DEBUG: Initial script environment"
echo "=========================================="
echo "DEBUG: Script start time: $(date)"
echo "DEBUG: Script arguments: $*"
echo "DEBUG: Build mode: $build_mode"
echo "DEBUG: JAR URL: ${jar_url:-'not provided'}"
echo "DEBUG: Initial working directory: $(pwd)"
echo "DEBUG: Shell: $SHELL"
echo "DEBUG: Script process ID: $$"

# Debug: Initial Python environment
echo "DEBUG: Initial Python environment:"
echo "  which python: $(which python 2>/dev/null || echo 'not found')"
echo "  which python3: $(which python3 2>/dev/null || echo 'not found')"
echo "  which pip: $(which pip 2>/dev/null || echo 'not found')"
echo "  python version: $(python --version 2>&1 || echo 'python not available')"
echo "  python3 version: $(python3 --version 2>&1 || echo 'python3 not available')"

# Debug: Check what imports prepackage_mgr.py needs
if [ -f "src/spark_rapids_pytools/resources/dev/prepackage_mgr.py" ]; then
  echo "DEBUG: Dependencies needed by prepackage_mgr.py:"
  grep -E "^import |^from " src/spark_rapids_pytools/resources/dev/prepackage_mgr.py | head -10
fi

echo "=========================================="

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
