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

# Usage: ./cleanup_hdfs.sh

readonly CURRENT_FILE_PATH=${0:a}

load_common_scripts() {
  local scripts_dir=$(dirname "$(dirname "${CURRENT_FILE_PATH}")")
  source "${scripts_dir}/common.sh"
}

# Stop HDFS services
stop_hdfs_services() {
    if jps | grep -q "NameNode\|DataNode"; then
        echo "Stopping HDFS..."
        local hadoop_home="${_HDFS_DIR}/hadoop-${HADOOP_VERSION}"
        local hdfs_bin="${hadoop_home}/bin/hdfs"
        [ ! -f "${hdfs_bin}" ] && err "HDFS binary not found at ${hdfs_bin}. However, HDFS services are running."
        $hdfs_bin --daemon stop namenode
        $hdfs_bin --daemon stop datanode
    else
        echo "HDFS is not running."
    fi
}

cleanup_hdfs_dir() {
    rm -rf "${_HDFS_DIR}"
    echo "Removed HDFS directories."
}

main() {
    load_common_scripts
    stop_hdfs_services
    cleanup_hdfs_dir
}

main
