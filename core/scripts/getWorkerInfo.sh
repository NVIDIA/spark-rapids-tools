#!/bin/bash
#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
#

# This script copies and executes discoveryScript.sh to the executor and copies
# back the generated YAML file. The YAML file is used by the AutoTuner for
# recommending Spark RAPIDS configurations.
# Assumption: 'discoveryScript.sh' is present in the same directory as this script.

# Usage: ./getWorkerInfo.sh [num-executors] [executor-ip] [output-file]

function usage() {
  echo "Usage: ./getWorkerInfo.sh [num-executors] [executor-ip] [output-file]"
}

if [ "$#" -ne 3 ]; then
  echo "Illegal number of parameters"
  usage
  exit 1
fi

NUM_EXECUTORS=$1
EXECUTOR_IP=$2
OUTPUT_FILE_ON_DRIVER=$3
OUTPUT_FILE_ON_EXECUTOR=/tmp/system_props.yaml
DISCOVERY_SCRIPT=discoveryScript.sh

echo "Fetching system information from executor - $EXECUTOR_IP"
scp -q ./$DISCOVERY_SCRIPT "$EXECUTOR_IP":/tmp
ssh "$EXECUTOR_IP" "bash /tmp/$DISCOVERY_SCRIPT $NUM_EXECUTORS $OUTPUT_FILE_ON_EXECUTOR"
scp -q "$EXECUTOR_IP":$OUTPUT_FILE_ON_EXECUTOR $OUTPUT_FILE_ON_DRIVER
echo -e "\nYAML file copied to driver at $OUTPUT_FILE_ON_DRIVER"
