#!/bin/bash
#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
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

# Internal script used by diagnostic tool to collect info from cluster node, such as
# OS version, Yarn configuration, Spark version and error logs etc.

set -e

PREFIX=${PREFIX:-`date +%Y%m%d%H%M`}
TEMP_PATH="/tmp/$PREFIX"

# Prepare temp folder to keep collected info
mkdir -p $TEMP_PATH

# Set output file to keep node info
NODE_ID=`hostname`
OUTPUT_NODE_INFO="$TEMP_PATH/$HOSTNAME.info"

if [[ "$PLATFORM_TYPE" == *"databricks"* ]]; then
    DATABRICKS_HOME='/databricks'
    SPARK_HOME="$DATABRICKS_HOME/spark"
else
    SPARK_HOME='/usr/lib/spark'
fi

echo "[OS version]" >> $OUTPUT_NODE_INFO
cat /etc/os-release >> $OUTPUT_NODE_INFO

echo "" >> $OUTPUT_NODE_INFO
echo "[Kernel version]" >> $OUTPUT_NODE_INFO
uname -a >> $OUTPUT_NODE_INFO

echo "" >> $OUTPUT_NODE_INFO
echo "[CPU info]" >> $OUTPUT_NODE_INFO
echo "# of cores: `nproc --all`" >> $OUTPUT_NODE_INFO
cat /proc/cpuinfo | grep 'model name' | head -n 1 >> $OUTPUT_NODE_INFO

echo "" >> $OUTPUT_NODE_INFO
echo "[Memory info]" >> $OUTPUT_NODE_INFO
free -h >> $OUTPUT_NODE_INFO

echo "" >> $OUTPUT_NODE_INFO
echo "[Network adapter]" >> $OUTPUT_NODE_INFO

if command -v lshw ; then
    lshw -C network >> $OUTPUT_NODE_INFO
else
    # Downgrade to 'lspci'
    if [[ "$PLATFORM_TYPE" == *"databricks"* ]]; then
        if command -v sudo ; then
            sudo apt install -y pciutils
        fi
        if command -v lspci ; then
            lspci | { grep 'Ethernet controller' || true; } >> $OUTPUT_NODE_INFO
        else
             echo 'not found' >> $OUTPUT_NODE_INFO
        fi
    elif [ "$PLATFORM_TYPE" == "emr" ]; then
        if command -v /usr/sbin/lspci ; then
            /usr/sbin/lspci | { grep 'Ethernet controller' || true; } >> $OUTPUT_NODE_INFO
        else
             echo 'not found' >> $OUTPUT_NODE_INFO
        fi
    fi
fi

echo "" >> $OUTPUT_NODE_INFO
echo "[Disk info]" >> $OUTPUT_NODE_INFO
lsblk >> $OUTPUT_NODE_INFO

echo "" >> $OUTPUT_NODE_INFO
echo "[GPU adapter]" >> $OUTPUT_NODE_INFO
if command -v lshw ; then
    lshw -C display >> $OUTPUT_NODE_INFO
else
    # Downgrade to 'lspci'
    if [[ "$PLATFORM_TYPE" == *"databricks"* ]]; then
        lspci | { grep '3D controller' || true; } >> $OUTPUT_NODE_INFO
    elif [ "$PLATFORM_TYPE" == "emr" ]; then
        /usr/sbin/lspci | { grep '3D controller' || true; } >> $OUTPUT_NODE_INFO
    fi
fi

echo "" >> $OUTPUT_NODE_INFO
echo "[GPU driver]" >> $OUTPUT_NODE_INFO
if command -v nvidia-smi ; then
    nvidia-smi >> $OUTPUT_NODE_INFO
else
    echo "not found command 'nvidia-smi'" >> $OUTPUT_NODE_INFO
fi

echo "" >> $OUTPUT_NODE_INFO
echo "[Java version]" >> $OUTPUT_NODE_INFO
java -version 2>> $OUTPUT_NODE_INFO

echo "" >> $OUTPUT_NODE_INFO
echo "[Spark version]" >> $OUTPUT_NODE_INFO

if [[ "$PLATFORM_TYPE" == *"databricks"* ]]; then
    if [ -f $SPARK_HOME/VERSION ]; then
        echo "$(cat "$SPARK_HOME/VERSION")" >> $OUTPUT_NODE_INFO
    else
        echo 'not found' >> $OUTPUT_NODE_INFO
    fi
else
    if command -v $SPARK_HOME/bin/pyspark ; then
        $SPARK_HOME/bin/pyspark --version 2>&1|grep -v Scala|awk '/version\ [0-9.]+/{print $NF}' >> $OUTPUT_NODE_INFO
    else
        echo 'not found' >> $OUTPUT_NODE_INFO
    fi
fi

echo "" >> $OUTPUT_NODE_INFO
echo "[Spark rapids plugin]" >> $OUTPUT_NODE_INFO

# Determine the appropriate path based on the platform
if [[ "$PLATFORM_TYPE" == *"databricks"* ]]; then
    jar_dir="$DATABRICKS_HOME/jars"
else
    jar_dir="$SPARK_HOME/jars"
fi

# Check for the jar file(s) and output the result
found_files=0

# Use for loop to handle potential multiple jar files
for jar_file in "$jar_dir"/rapids-4-spark*.jar; do
    # Check if the file exists (handles case where no files match)
    if [ -f "$jar_file" ]; then
        ls -l "$jar_file" >> "$OUTPUT_NODE_INFO"
        found_files=1
    fi
done

# If no files were found, output 'not found'
if [ $found_files -eq 0 ]; then
    echo 'not found' >> "$OUTPUT_NODE_INFO"
fi

echo "" >> $OUTPUT_NODE_INFO
echo "[CUDA version]" >> $OUTPUT_NODE_INFO
if [ -f /usr/local/cuda/version.json ]; then
    cat  /usr/local/cuda/version.json | grep '\<cuda\>' -A 2 | grep version >> $OUTPUT_NODE_INFO
else
    # Fetch CUDA version from nvidia-smi
    if command -v nvidia-smi ; then
        nvidia-smi --query | grep "CUDA Version" | awk '{print $4}' >> $OUTPUT_NODE_INFO
    else
        echo 'not found' >> $OUTPUT_NODE_INFO
    fi
fi

# Copy config files
CONFIGS="/etc/spark/conf/spark-defaults.conf
/etc/spark/conf/spark-env.sh
/etc/hadoop/conf/core-site.xml
/etc/hadoop/conf/hadoop-env.sh
/etc/hadoop/conf/hdfs-site.xml
/etc/hadoop/conf/mapred-site.xml
/etc/hadoop/conf/yarn-env.sh
/etc/hadoop/conf/yarn-site.xml
"

for i in $CONFIGS ; do
    if [ -f $i ]; then
        cp $i $TEMP_PATH
    else
        echo "not found $i"
    fi
done

# Create archive for collected info
tar cfz ${TEMP_PATH}_info.tgz $TEMP_PATH
echo "Archive '${TEMP_PATH}_info.tgz' is successfully created!"

# Create archive for log files
# Note,
# 1. sudo privilege is required to access log files
# 2. exclude core files in pattern: *.out or *.out.*
# 3. exclude 'lastlog' which will block tar command on Dataproc
# 4. ignore exit code 1 as it happened if found file changed during read
cd /var/log && sudo tar --exclude='*.out' --exclude='*.out.*' --exclude='lastlog' --warning=no-file-changed -zcf "${TEMP_PATH}_log.tgz" -- * || [[ $? -eq 1 ]]
echo "Archive '${TEMP_PATH}_log.tgz' is successfully created!"
