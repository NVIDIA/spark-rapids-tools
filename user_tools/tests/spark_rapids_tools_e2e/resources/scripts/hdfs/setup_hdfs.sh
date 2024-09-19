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

# This script sets up and configures Hadoop HDFS. It optionally starts HDFS services
# based on the provided HDFS_SHOULD_RUN flag.
#
# HDFS Configuration:
# - Replication factor: 1
# - Disk Space Quota: 2GB
# - Temp Directory: /tmp/spark_rapids_tools_e2e_tests
#
# Usage: ./setup_hdfs.sh --run|--no-run
# Options:
#   --run     Run HDFS services (default)
#   --no-run  Do not run HDFS

set -e

usage() {
    echo "Usage: $0 --run|--no-run" >&2
    echo "Options:"
    echo "  --run     Run HDFS services (default)" >&2
    echo "  --no-run  Do not run HDFS" >&2
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --run)
            readonly HDFS_SHOULD_RUN=true
            shift
            ;;
        --no-run)
            readonly HDFS_SHOULD_RUN=false
            shift
            ;;
        *)
            echo "Invalid option: $1" >&2
            usage
            ;;
    esac
done

echo "HDFS_SHOULD_RUN: ${HDFS_SHOULD_RUN}"
readonly DEFAULT_CORE_SITE_XML="core-site.xml"
readonly DEFAULT_HDFS_SITE_XML="hdfs-site.xml"
readonly HDFS_SPACE_QUOTA="2g"
readonly CURRENT_FILE_PATH=$(realpath "${0}")
readonly HDFS_SCRIPTS_DIR=$(dirname "${CURRENT_FILE_PATH}")
readonly VERIFY_HDFS_SERVICES_MAX_RETRY=3
readonly VERIFY_HDFS_SERVICES_SLEEP_SEC=5

load_common_scripts() {
  local scripts_dir=$(dirname "${HDFS_SCRIPTS_DIR}")
  source "${scripts_dir}/common.sh"
}

# Validate environment variables and dependencies
validate_env() {
    [ -z "${JAVA_HOME}" ] && err "JAVA_HOME is not set. Please set JAVA_HOME."
    [ -z "${E2E_TEST_HADOOP_VERSION}" ] && err "E2E_TEST_HADOOP_VERSION is not set. Please set E2E_TEST_HADOOP_VERSION."
    [ -z "${E2E_TEST_TMP_DIR}" ] && err "E2E_TEST_TMP_DIR is not set. Please set E2E_TEST_TMP_DIR (e.g. /tmp/spark_rapids_tools_e2e_tests)."
    command -v jps >/dev/null || err "jps is not available. Please install JDK or add JDK bin directory to PATH."
}

# Set up HDFS directories
setup_hdfs_dirs() {
    echo "Setting up HDFS directories..."
    readonly E2E_TEST_NAME_NODE_DIR="${E2E_TEST_HDFS_DIR}/namenode"
    readonly E2E_TEST_DATA_NODE_DIR="${E2E_TEST_HDFS_DIR}/datanode"
    rm -rf "${E2E_TEST_HDFS_DIR}" "${E2E_TEST_NAME_NODE_DIR}" "${E2E_TEST_DATA_NODE_DIR}"
    mkdir -p "${E2E_TEST_HDFS_DIR}" "${E2E_TEST_NAME_NODE_DIR}" "${E2E_TEST_DATA_NODE_DIR}"
    export E2E_TEST_NAME_NODE_DIR E2E_TEST_DATA_NODE_DIR
}

# Function to verify checksum
verify_checksum() {
    echo "Verifying checksum..."
    if [ $# -ne 2 ]; then
        err "verify_checksum requires two arguments: file and checksum_file."
    fi
    local file="$1"
    local checksum_file="$2"
    local expected_checksum=$(awk '{print $4}' "${checksum_file}")
    local actual_checksum=$(shasum -a 512 "${file}" | awk '{print $1}')
    [ "${expected_checksum}" != "${actual_checksum}" ] && return 1 || return 0
}

# Function to download and extract Hadoop
download_and_extract_hadoop() {
    echo "Downloading and extracting Hadoop..."
    local hadoop_url="https://dlcdn.apache.org/hadoop/common/hadoop-${E2E_TEST_HADOOP_VERSION}/hadoop-${E2E_TEST_HADOOP_VERSION}.tar.gz"
    local hadoop_tar_file="${E2E_TEST_TMP_DIR}/hadoop-${E2E_TEST_HADOOP_VERSION}.tar.gz"
    local checksum_url="${hadoop_url}.sha512"
    local checksum_file="${hadoop_tar_file}.sha512"

    if [ ! -f "${hadoop_tar_file}" ]; then
        wget -O "${hadoop_tar_file}" "${hadoop_url}" || err "Failed to download Hadoop tarball."
    fi

    # Verify checksum and re-download if needed
    wget -O "${checksum_file}" "${checksum_url}" || err "Failed to download checksum file."
    if ! verify_checksum "${hadoop_tar_file}" "${checksum_file}"; then
        wget -O "${hadoop_tar_file}" "${hadoop_url}" || err "Failed to download Hadoop tarball."
        if ! verify_checksum "${hadoop_tar_file}" "${checksum_file}"; then
            err "Checksum verification failed after re-downloading. Exiting..."
        fi
    fi

    tar -xzf "${hadoop_tar_file}" -C "${E2E_TEST_HDFS_DIR}" || err "Failed to extract Hadoop tarball."
}

# Configure Hadoop
configure_hadoop() {
    echo "Configuring Hadoop..."
    readonly HADOOP_HOME="${E2E_TEST_HDFS_DIR}/hadoop-${E2E_TEST_HADOOP_VERSION}"
    readonly HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_HOME HADOOP_CONF_DIR
    export PATH="${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"
    envsubst < "${HDFS_SCRIPTS_DIR}/templates/${DEFAULT_CORE_SITE_XML}" > "${HADOOP_HOME}/etc/hadoop/core-site.xml"
    envsubst < "${HDFS_SCRIPTS_DIR}/templates/${DEFAULT_HDFS_SITE_XML}" > "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml"
}

# Format the Namenode
format_namenode() {
    echo "Formatting the Namenode..."
    yes | hdfs namenode -format || err "Failed to format Namenode."
}

# Start HDFS services
start_hdfs_services() {
    echo "Starting HDFS services..."
    hdfs --daemon start namenode
    hdfs --daemon start datanode
}

# Verify that HDFS services are running
verify_hdfs_services() {
    echo "Verifying HDFS services..."
    jps | grep -q "NameNode" || err "Namenode is not running."
    jps | grep -q "DataNode" || err "Datanode is not running."
    hdfs dfs -ls / || err "Failed to list HDFS root directory."
    hdfs dfsadmin -setSpaceQuota "${HDFS_SPACE_QUOTA}" / || err "Failed to set space quota of ${HDFS_SPACE_QUOTA}"
    hdfs dfsadmin -report || err "Failed to get HDFS report."
}

verify_hdfs_services_with_retry() {
    local max_retry=$1
    local count=1
    while [[ ${count} -le ${max_retry} ]]; do
        echo "Attempt ${count} of ${max_retry}..."
        if verify_hdfs_services; then
            echo "HDFS services are running."
            return 0
        fi
        echo "HDFS services verification failed. Retrying in ${VERIFY_HDFS_SERVICES_SLEEP_SEC} seconds..."
        sleep ${VERIFY_HDFS_SERVICES_SLEEP_SEC}
        ((count++))
    done
    return 1
}

main() {
    load_common_scripts
    validate_env
    setup_hdfs_dirs
    download_and_extract_hadoop
    configure_hadoop
    if [ "${HDFS_SHOULD_RUN}" = true ]; then
      format_namenode
      start_hdfs_services
      verify_hdfs_services_with_retry ${VERIFY_HDFS_SERVICES_MAX_RETRY} || err "Failed to start HDFS services after ${VERIFY_HDFS_SERVICES_MAX_RETRY} attempts."
    fi
    echo "${HADOOP_HOME}"
}

main
