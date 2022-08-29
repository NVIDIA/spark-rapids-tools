# Copyright (c) 2022, NVIDIA CORPORATION.
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
import re
from dataclasses import dataclass
import subprocess
import sys
import yaml

# Maximum amount of pinned memory to use per executor in megabytes
MAX_PINNED_MEMORY_MB = 4 * 1024

# Maximum number of concurrent tasks to run on the GPU
MAX_GPU_CONCURRENT = 4

# Amount of GPU memory to use per concurrent task in megabytes
# Using a bit less than 8GB here since Dataproc clusters advertise
# T4s as only having around 14.75 GB and we want to run with
# 2 concurrent by default on T4s.
GPU_MEM_PER_TASK_MB = 7500

# Ideal amount of JVM heap memory to request per CPU core in megabytes
HEAP_PER_CORE_MB = 2 * 1024

# Fraction of the executor JVM heap size that should be additionally reserved
# for JVM off-heap overhead (thread stacks, native libraries, etc.)
HEAP_OVERHEAD_FRACTION = 0.1

# Amount of CPU memory to reserve for system overhead (kernel, buffers, etc.) in megabytes
SYSTEM_RESERVE_MB = 2 * 1024


@dataclass
class WorkerInfo:
    num_cpus: int
    cpu_mem: int
    num_gpus: int
    gpu_mem: int


def fatal_error(msg):
    """
    Prints the specified message to stderr and exits with a non-zero exit code.
    """
    print(msg, file=sys.stderr)
    sys.exit(1)


def check_subprocess_result(c):
    """
    Inspects the specified CompletedProcess object to see if the subprocess
    was successful. If the subprocess was not successful then an error message
    is printed and the process exits.
    :param c: CompletedProcess returned from subprocess execution
    """
    if c.returncode != 0:
        fatal_error(f"Error invoking: {c.args}\n{c.stdout}{c.stderr}")


def get_shell_cmd_output(cmd):
    """
    Runs the specified string as a shell command and returns the captured stdout
    from the subprocess. If the shell command exits with a non-zero exit code
    then an error message is printed and the process exits.
    :param cmd: string containing the shell command to execute
    :return: the captured stdout from the successfully executed shell command
    """
    c = subprocess.run(cmd, capture_output=True, shell=True, text=True)
    check_subprocess_result(c)
    return c.stdout


def get_cluster_config(cluster_name, region):
    """
    Retrieves the Dataproc configuration properties for the specified cluster
    in the specified region.
    :param cluster_name: name of the Dataproc cluster
    :param region: region of the Dataproc cluster
    :return: dictionary of cluster properties
    """
    output = get_shell_cmd_output(
        f"gcloud dataproc clusters describe {cluster_name} --region={region}")
    return yaml.safe_load(output)["config"]


def get_zone(config):
    """
    Extracts the zone of a Dataproc cluster from the cluster's configuration.
    :param config: dictionary containing the Dataproc cluster configuration
    :return: cluster zone as a string
    """
    zoneuri = dict(config["gceClusterConfig"])["zoneUri"]
    return zoneuri[zoneuri.rindex("/") + 1:]


def get_worker_gpu_info(config):
    """
    Returns information on the GPUs assigned to each worker in a Dataproc cluster.
    :param config: dictionary containing the Dataproc cluster configuration
    :return: pair containing GPU count per worker and individual GPU memory size in megabytes
    """
    zone = get_zone(config)
    worker_config = dict(config["workerConfig"])
    worker = worker_config["instanceNames"][0]

    gpu_info = get_shell_cmd_output(
        f"gcloud compute ssh {worker} --zone={zone} "
        "--command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'")
    # sometimes the output of the command may include SSH warning messages.
    # match only lines in with expression in the following format "15109 MiB"
    match_arr = re.findall("(\d+)\s+(MiB)", gpu_info, flags=re.MULTILINE)
    num_gpus = len(match_arr)
    gpu_mem = 0
    if num_gpus == 0:
        fatal_error(f"Unrecognized GPU memory output format: {gpu_info}")
    for (mem_size, mem_unit) in match_arr:
        gpu_mem = max(int(mem_size), gpu_mem)
    return num_gpus, gpu_mem


def decode_machine_type_uri(uri):
    """
    Extracts the zone and machine type strings from a Dataproc machine-type URI.
    :param uri: machine-type URI
    :return: pair containing zone and machine-type strings
    """
    uri_parts = uri.split("/")[-4:]
    if uri_parts[0] != "zones" or uri_parts[2] != "machineTypes":
        fatal_error(f"Unable to parse machine type from machine type URI: {uri}")
    zone = uri_parts[1]
    machine_type = uri_parts[3]
    return zone, machine_type


def get_worker_cpu_info(cluster_config):
    """
    Extract CPU information about the worker nodes from a Dataproc cluster's configuration properties.
    :param cluster_config: dictionary of cluster configuration properties
    :return: pair containing the number of CPUs per worker node and memory per worker node in megabytes
    """
    type_uri = cluster_config["workerConfig"]["machineTypeUri"]
    zone, machine_type = decode_machine_type_uri(type_uri)
    output = get_shell_cmd_output(
        f"gcloud compute machine-types describe {machine_type} --zone={zone}")
    type_config = yaml.safe_load(output)
    num_cpus = type_config["guestCpus"]
    cpu_mem = type_config["memoryMb"]
    return num_cpus, cpu_mem


def get_worker_info(config):
    """
    Extract the CPU and GPU information for a worker node from a
    Dataproc cluster's configuration properties.
    :param config: dictionary of cluster configuration properties
    :return: WorkerInfo object
    """
    num_cpus, cpu_mem = get_worker_cpu_info(config)
    num_gpus, gpu_mem = get_worker_gpu_info(config)
    return WorkerInfo(num_cpus=num_cpus, cpu_mem=cpu_mem, num_gpus=num_gpus, gpu_mem=gpu_mem)


def get_spark_bootstrap_settings(worker_info, cluster_name):
    executors_per_node = worker_info.num_gpus
    num_executor_cores = max(1, worker_info.num_cpus // executors_per_node)
    task_gpu_amount = 1 / num_executor_cores
    gpu_concurrent_tasks = min(MAX_GPU_CONCURRENT, worker_info.gpu_mem // GPU_MEM_PER_TASK_MB)

    # account for system overhead
    usable_worker_mem = max(0, worker_info.cpu_mem - SYSTEM_RESERVE_MB)
    executor_container_mem = usable_worker_mem // executors_per_node

    # reserve 10% of heap as memory overhead
    max_executor_heap = max(0, int(executor_container_mem * (1 - HEAP_OVERHEAD_FRACTION)))

    # give up to 2GB of heap to each executor core
    executor_heap = min(max_executor_heap, HEAP_PER_CORE_MB * num_executor_cores)
    executor_mem_overhead = int(executor_heap * HEAP_OVERHEAD_FRACTION)

    # pinned memory uses any unused space up to 4GB
    pinned_mem = min(MAX_PINNED_MEMORY_MB, executor_container_mem - executor_heap - executor_mem_overhead)
    executor_mem_overhead += pinned_mem

    return f"""
##### BEGIN : RAPIDS bootstrap settings for {cluster_name}
spark.executor.cores={num_executor_cores}
spark.executor.memory={executor_heap}m
spark.executor.memoryOverhead={executor_mem_overhead}m
spark.rapids.sql.concurrentGpuTasks={gpu_concurrent_tasks}
spark.rapids.memory.pinnedPool.size={pinned_mem}m
spark.sql.files.maxPartitionBytes=512m
spark.task.resource.gpu.amount={task_gpu_amount}
##### END : RAPIDS bootstrap settings for {cluster_name}
"""


def get_bootstrap_configs(cluster_name, cluster_config):
    """
    Get the recommended Spark default settings for the specifed Dataproc cluster and region.
    :param cluster_name: name of the Dataproc cluster
    :param cluster_config: dictionary containing the Dataproc cluster properties
    :return: string containing the recommended Spark default settings for the cluster
    """
    worker_info = get_worker_info(cluster_config)
    return get_spark_bootstrap_settings(worker_info, cluster_name)


def update_driver_nodes(cluster_config, spark_settings):
    """
    Update the Spark default configuration on a Dataproc cluster's driver nodes.
    :param cluster_config: dictionary containing the Dataproc cluster properties
    :param spark_settings: string containing the Spark configuration settings to add
    """
    driver_config = cluster_config["masterConfig"]
    driver_zone = decode_machine_type_uri(driver_config["machineTypeUri"])[0]
    for node in driver_config["instanceNames"]:
        print(f"Updating configs on node {node}")
        c = subprocess.run(
            f"gcloud compute ssh {node} --zone={driver_zone} "
            "--command=\"sudo bash -c 'cat >> /etc/spark/conf/spark-defaults.conf'\"",
            capture_output=True, input=spark_settings, shell=True, text=True)
        check_subprocess_result(c)


def bootstrap_optimize(cluster_name, region):
    """
    Optimize the Spark default settings for the specified Dataproc cluster and region
    :param cluster_name: name of the Dataproc cluster
    :param region: region of the Dataproc cluster
    """
    cluster_config = get_cluster_config(cluster_name, region)
    spark_settings = get_bootstrap_configs(cluster_name, cluster_config)
    print("Using the following computed settings based on worker nodes:")
    print(spark_settings)
    update_driver_nodes(cluster_config, spark_settings)


if __name__ == "__main__":
    bootstrap_optimize(sys.argv[1], sys.argv[2])
