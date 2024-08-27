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

# Usage: ./setup_hdfs.sh [_HDFS_SHOULD_RUN]

_HDFS_SHOULD_RUN="True"

if [ "$#" -eq 1 ]; then
    _HDFS_SHOULD_RUN=$1
else
    echo "_HDFS_SHOULD_RUN is not provided. Defaulting to True."
fi

readonly DEFAULT_CORE_SITE_XML="core-site.xml"
readonly DEFAULT_HDFS_SITE_XML="hdfs-site.xml"
readonly CURRENT_FILE_PATH=$(realpath "${0}")
readonly HDFS_SCRIPTS_DIR=$(dirname "${CURRENT_FILE_PATH}")

load_common_scripts() {
  local scripts_dir=$(dirname "${HDFS_SCRIPTS_DIR}")
  source "${scripts_dir}/common.sh"
}

# Validate environment variables
validate_env() {
    [ -z "${JAVA_HOME}" ] && err "JAVA_HOME is not set. Please set JAVA_HOME."
    [ -z "${HADOOP_VERSION}" ] && err "HADOOP_VERSION is not set. Please set HADOOP_VERSION."
}

# Set up HDFS directories
setup_hdfs_dirs() {
    echo "Setting up HDFS directories..."
    readonly NAME_NODE_DIR="${_HDFS_DIR}/namenode"
    readonly DATA_NODE_DIR="${_HDFS_DIR}/datanode"
    rm -rf "${_HDFS_DIR}" "${NAME_NODE_DIR}" "${DATA_NODE_DIR}"
    mkdir -p "${_HDFS_DIR}" "${NAME_NODE_DIR}" "${DATA_NODE_DIR}"
    export NAME_NODE_DIR DATA_NODE_DIR
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
    local hadoop_url="https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
    local hadoop_tar_file="${TOOLS_TMP_DIR}/hadoop-${HADOOP_VERSION}.tar.gz"
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

    tar -xzf "${hadoop_tar_file}" -C "${_HDFS_DIR}" || err "Failed to extract Hadoop tarball."
}

# Configure Hadoop
configure_hadoop() {
    echo "Configuring Hadoop..."
    readonly HADOOP_HOME="${_HDFS_DIR}/hadoop-${HADOOP_VERSION}"
    readonly HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_HOME HADOOP_CONF_DIR
    export PATH="${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"
    envsubst < "${HDFS_SCRIPTS_DIR}/templates/${DEFAULT_CORE_SITE_XML}" > "${HADOOP_HOME}/etc/hadoop/core-site.xml"
    envsubst < "${HDFS_SCRIPTS_DIR}/templates/${DEFAULT_HDFS_SITE_XML}" > "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml"
}

# Format the Namenode
format_namenode() {
    echo "Formatting the Namenode..."
    yes | hdfs namenode -format > /dev/null 2>&1  || err "Failed to format Namenode."
}

# Start HDFS services
start_hdfs_services() {
    echo "Starting HDFS services..."
    hdfs --daemon start namenode || err "Failed to start Namenode."
    hdfs --daemon start datanode || err "Failed to start Datanode."
}

# Verify that HDFS services are running
verify_hdfs_services() {
    echo "Verifying HDFS services..."
    sleep 5
    jps | grep -q "NameNode" || err "Namenode is not running."
    jps | grep -q "DataNode" || err "Datanode is not running."
    hdfs dfs -ls / || err "Failed to list HDFS root directory."
    hdfs dfsadmin -report || err "Failed to get HDFS report."
}

main() {
    load_common_scripts
    validate_env
    setup_hdfs_dirs
    download_and_extract_hadoop
    configure_hadoop
    if [ "${_HDFS_SHOULD_RUN}" = "True" ]; then
      format_namenode
      start_hdfs_services
      verify_hdfs_services
    fi
    echo "${HADOOP_HOME}"
}

main
