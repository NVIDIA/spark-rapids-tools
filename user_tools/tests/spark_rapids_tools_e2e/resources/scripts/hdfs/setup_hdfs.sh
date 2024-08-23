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

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <should_run>"
    exit 1
fi

SHOULD_RUN=$1
DEFAULT_CORE_SITE_XML="core-site.xml"
DEFAULT_HDFS_SITE_XML="hdfs-site.xml"

# Ensure SCRIPTS_DIR is set
set_scripts_dir() {
    if [ -z "$SCRIPTS_DIR" ]; then
        CURRENT_FILE_PATH=$(realpath "${BASH_SOURCE[0]}")
        SCRIPTS_DIR=$(dirname "$(dirname "$CURRENT_FILE_PATH")")
    fi
    HDFS_SCRIPTS_DIR="${SCRIPTS_DIR}/hdfs"
    source "$HDFS_SCRIPTS_DIR/common.sh"
}

# Validate environment variables
validate_env() {
    [ -z "${JAVA_HOME}" ] && err "JAVA_HOME is not set. Please set JAVA_HOME."
}

# Set up HDFS directories
setup_hdfs_dirs() {
    echo "Setting up HDFS directories..."
    export name_node_dir="${hdfs_dir}/namenode"
    export data_node_dir="${hdfs_dir}/datanode"
    mkdir -p "${hdfs_dir}" "${name_node_dir}" "${data_node_dir}"
}

# Download and extract Hadoop
download_and_extract_hadoop() {
    echo "Downloading and extracting Hadoop..."
    local hadoop_url="https://dlcdn.apache.org/hadoop/common/hadoop-${hadoop_version}/hadoop-${hadoop_version}.tar.gz"
    local hadoop_tar_file="/tmp/hadoop-${hadoop_version}.tar.gz"

    if [ ! -f "$hadoop_tar_file" ]; then
        wget -O "$hadoop_tar_file" "${hadoop_url}" || err "Failed to download Hadoop tarball."
    fi

    tar -xzf "$hadoop_tar_file" -C "${hdfs_dir}" || err "Failed to extract Hadoop tarball."
}

# Configure Hadoop
configure_hadoop() {
    echo "Configuring Hadoop..."
    export HADOOP_HOME="${hdfs_dir}/hadoop-${hadoop_version}"
    export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    envsubst < "$HDFS_SCRIPTS_DIR/templates/$DEFAULT_CORE_SITE_XML" > "${HADOOP_HOME}/etc/hadoop/core-site.xml"
    envsubst < "$HDFS_SCRIPTS_DIR/templates/$DEFAULT_HDFS_SITE_XML" > "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml"
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
    set_scripts_dir
    validate_env
    source "${HDFS_SCRIPTS_DIR}/common.sh"
    setup_hdfs_dirs
    download_and_extract_hadoop
    configure_hadoop
    if [ "$SHOULD_RUN" = "True" ]; then
      format_namenode
      start_hdfs_services
      verify_hdfs_services
    fi
    echo "$HADOOP_HOME"
}

main
