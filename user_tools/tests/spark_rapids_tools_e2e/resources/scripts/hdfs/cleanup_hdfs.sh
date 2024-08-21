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

set_scripts_dir() {
    if [ -z "$SCRIPTS_DIR" ]; then
        CURRENT_FILE_PATH=$(realpath "${BASH_SOURCE[0]}")
        SCRIPTS_DIR=$(dirname "$(dirname "$CURRENT_FILE_PATH")")
    fi
    HDFS_SCRIPTS_DIR="${SCRIPTS_DIR}/hdfs"
    source "${HDFS_SCRIPTS_DIR}/common.sh"
}

# Stop HDFS services
stop_hdfs_services() {
    if jps | grep -q "NameNode\|DataNode"; then
        echo "Stopping HDFS..."
        HADOOP_HOME="${hdfs_dir}/hadoop-${hadoop_version}"
        hdfs_bin="${HADOOP_HOME}/bin/hdfs"
        [ ! -f "$hdfs_bin" ] && err "HDFS binary not found at ${hdfs_bin}. However, HDFS services are running."
        $hdfs_bin --daemon stop namenode
        $hdfs_bin --daemon stop datanode
    else
        echo "HDFS is not running."
    fi
}

cleanup_hdfs() {
    rm -rf "${hdfs_dir}"
    echo "HDFS has been stopped and cleaned up."
}

# Main function to orchestrate the script
main() {
    set_scripts_dir
    stop_hdfs_services
    cleanup_hdfs
}

# Execute the main function
main
