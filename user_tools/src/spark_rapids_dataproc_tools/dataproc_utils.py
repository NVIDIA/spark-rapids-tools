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

import os
import re
import subprocess
from dataclasses import dataclass, field
from logging import Logger
from typing import Optional

import yaml

from spark_rapids_dataproc_tools.utilities import is_system_tool, bail, YAMLPropertiesContainer

is_mac = os.uname().sysname == 'Darwin'


def validate_dataproc_sdk():
    """
    Make sure that the environment is set correctly.
    gcloud and gsutil scripts must be part of the path
    """
    tool_list = ['gcloud', 'gsutil']
    try:
        for tool in tool_list:
            if not is_system_tool(tool):
                raise ValueError("Tool {} is not in installed or not in the PATH environment".format(tool))
    except ValueError as err:
        bail('gcloud SDK check failed', err)


def get_default_region():
    return os.getenv('CLOUDSDK_DATAPROC_REGION')


def validate_region(region):
    try:
        if region is None:
            raise ValueError("""The required property [region] is not currently set.\n
                         \tIt can be set on a per-command basis by re-running your command with the [--region] flag.\n
                         \tOr it can be set temporarily by the environment variable [CLOUDSDK_DATAPROC_REGION]""")

    except ValueError as err:
        bail('Invalid arguments for region', err)


def parse_supported_gpu(value: str) -> Optional[str]:
    supported_list = ['T4', 'V100', 'K80', 'A100', 'P100']
    normalized = value.upper()
    for gpu_device in supported_list:
        if normalized.find(gpu_device) != -1:
            return gpu_device
    return None


# N2-* and E2-* machines do not support GPUs.
# we currently support only N1 machines
def is_machine_compatible_for_gpu(machine_type: str) -> bool:
    normalized_mc = machine_type.lower()
    return normalized_mc.startswith("n1-")


# This method converts machine types that don't support GPU on dataproc yet to
# n1 types. Right now, it's a very simple conversion where it tries to match the cores as close as
# possible while keeping the rest of the type unchanged
#
# e.g. 'n2-standard-8' -> 'n1-standard-8'
#      'n2-higmem-128' -> 'n1-highmem-96' as this is the highest one available
def map_to_closest_supported_match(machine_type: str) -> str:
    def get_core(core):
        for c in dataproc_n1_cores:
            if c >= core:
                return c
        return dataproc_n1_cores[-1]

    # todo assert that the machine_type is not supported
    dataproc_n1_cores = [2, 4, 8, 16, 32, 64, 96]
    instance_name_parts = machine_type.split("-")
    # The last bit of the instance name is the number of cores
    original_core = instance_name_parts[-1]
    return "n1-{}-{}".format(instance_name_parts[1], get_core(int(original_core)))


@dataclass
class CMDRunner(object):
    logger: Logger
    debug: bool
    fail_action = None

    def _check_subprocess_result(self, c, expected, msg_fail: str):
        try:
            if c.returncode != expected:
                raise Exception(f"Error invoking: {c.args}\n{c.stdout}{c.stderr}")
        except Exception as ex:
            if self.fail_action is None:
                if msg_fail:
                    bail(f"{msg_fail}", ex)
                bail(f'Error invoking cmd', ex)
            self.fail_action(ex, msg_fail)

    def run(self, cmd: str, expected: int = 0, fail_ok: bool = False,
            msg_fail: Optional[str] = None):
        c = subprocess.run(cmd, capture_output=True, shell=True, text=True)
        if not fail_ok:
            self._check_subprocess_result(c, expected=expected, msg_fail=msg_fail)
        if self.debug:
            stdout_dump = "" if len(c.stdout) == 0 else f" \n\t -> \n{c.stdout}"
            self.logger.debug(f'executed cmd: {cmd}.{stdout_dump}')
        return c.stdout

    def gcloud(self, cmd, expected: int = 0, msg_fail: Optional[str] = None, fail_ok: bool = False):
        gcloud_cmd = f'gcloud {cmd}'
        return self.run(gcloud_cmd, expected=expected, msg_fail=msg_fail, fail_ok=fail_ok)

    def gsutil(self, cmd, expected: int = 0, msg_fail: Optional[str] = None, fail_ok: bool = False):
        gsutil_cmd = f'gsutil {cmd}'
        return self.run(gsutil_cmd, expected=expected, msg_fail=msg_fail, fail_ok=fail_ok)

    def gcloud_rm(self, remote_path: str, is_dir: bool = True, fail_ok: bool = False):
        recurse = '-r' if is_dir else ''
        multi_threaded = "-m" if not is_mac else ""
        rm_cmd = f'{multi_threaded} rm -f {recurse} {remote_path}'
        self.gsutil(rm_cmd, fail_ok=fail_ok)

    def gcloud_cp(self, src_path: str,
                  dst_path: str,
                  overwrite: bool = True,
                  is_dir: bool = True,
                  fail_ok: bool = False):
        cp_cmd = '{} cp {} {} {} {}'.format('-m' if not is_mac else '',
                                            '' if overwrite else '-n',
                                            '-r' if is_dir else '',
                                            src_path,
                                            dst_path)
        self.gsutil(cp_cmd, fail_ok=fail_ok)


@dataclass
class DataprocClusterPropContainer(YAMLPropertiesContainer):
    cli: CMDRunner = None
    uuid: str = field(default=None, init=False)
    workers_count: int = field(default=None, init=False)

    def _init_fields(self) -> None:
        self.uuid = self.get_value('clusterUuid')
        self.workers_count = self.get_value('config', 'workerConfig', 'numInstances')

    def __decode_machine_type_uri(self, uri):
        uri_parts = uri.split("/")[-4:]
        if uri_parts[0] != "zones" or uri_parts[2] != "machineTypes":
            self.cli.fail_action(ValueError(f"Unable to parse machine type from machine type URI: {uri}"),
                                 "Failed while processing CPU info")
        zone_val = uri_parts[1]
        machine_type_val = uri_parts[3]
        return zone_val, machine_type_val

    def get_cpu_info_for_machine_type(self, zone: str, machine_type: str) -> (str, str):
        exec_cmd = f'compute machine-types describe {machine_type} --zone={zone}'
        raw_output = self.cli.gcloud(exec_cmd)
        type_config = yaml.safe_load(raw_output)
        num_cpus = type_config["guestCpus"]
        cpu_mem = type_config["memoryMb"]
        return num_cpus, cpu_mem

    def __get_cpu_info_for_node(self, node_type: str) -> (str, str):
        type_uri = self.get_value('config', '{}Config'.format(node_type), 'machineTypeUri')
        zone, machine_type = self.__decode_machine_type_uri(type_uri)
        return self.get_cpu_info_for_machine_type(zone=zone, machine_type=machine_type)

    def __get_local_ssds_for_node(self, node_type: str) -> int:
        ssds_count = self.get_value_silent('config', '{}Config'.format(node_type), 'diskConfig', 'numLocalSsds')
        if ssds_count is None:
            return 0
        return int(ssds_count)

    def __get_number_instances(self, node_type: str) -> int:
        return self.get_value_silent('config', '{}Config'.format(node_type), 'numInstances')

    def __get_machine_info_for_node(self, node_type: str) -> (str, str, str):
        """
        Extracts the machine type of Dataproc node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing  node region, zone, and type.
        """
        type_uri = self.get_value('config', '{}Config'.format(node_type), 'machineTypeUri')
        zone, machine_type = self.__decode_machine_type_uri(type_uri)
        zone_parts = zone.split("-")
        region = "{}-{}".format(zone_parts[0], zone_parts[1])
        return region, zone, machine_type

    def convert_worker_machine_if_not_supported(self) -> (str, bool):
        worker_region, worker_zone, worker_machine = self.get_worker_machine_info()
        if not is_machine_compatible_for_gpu(worker_machine):
            return map_to_closest_supported_match(worker_machine), True
        return worker_machine, False

    def get_master_vm_instances(self):
        return self.__get_number_instances('master')

    def get_worker_vm_instances(self):
        return self.__get_number_instances('worker')

    def get_master_local_ssds(self):
        return self.__get_local_ssds_for_node('master')

    def get_worker_local_ssds(self):
        return self.__get_local_ssds_for_node('worker')

    def get_master_machine_info(self) -> (str, str, str):
        """
        Extracts the machine type of Dataproc master node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing master node machine region, zone, and type.
        """
        return self.__get_machine_info_for_node('master')

    def get_worker_machine_info(self) -> (str, str, str):
        """
        Extracts the machine type of Dataproc master node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing master node machine region, zone, and type.
        """
        return self.__get_machine_info_for_node('worker')

    def get_zone(self) -> str:
        """
        Extracts the zone of a Dataproc cluster from the cluster's configuration.
        :return: cluster zone as a string
        """
        zoneuri = self.get_value('config', 'gceClusterConfig', 'zoneUri')
        return zoneuri[zoneuri.rindex("/") + 1:]

    def get_worker_gpu_device(self) -> str:
        zone = self.get_zone()
        worker = self.get_value('config', 'workerConfig', 'instanceNames')[0]
        gpu_cmd = (
            f'compute ssh {worker} --zone={zone} '
            f"--command='nvidia-smi --query-gpu=gpu_name --format=csv,noheader'"
        )
        gpu_devices = self.cli.gcloud(gpu_cmd)
        all_lines = gpu_devices.splitlines()
        if len(all_lines) == 0:
            self.cli.fail_action(ValueError(f"Unrecognized tesla device: {gpu_devices}"),
                                 "Failed while processing GpuDevice")
        for line in all_lines:
            gpu_device = parse_supported_gpu(line)
            if gpu_device is None:
                self.cli.fail_action(ValueError(f"Unrecognized tesla device: {line}"),
                                     "Failed while processing GpuDevice")
            return gpu_device

    def get_worker_gpu_info(self) -> (int, int):
        """
        Returns information on the GPUs assigned to each worker in a Dataproc cluster.
        :return: pair containing GPU count per worker and individual GPU memory size in megabytes
        """
        zone = self.get_zone()
        worker = self.get_value('config', 'workerConfig', 'instanceNames')[0]
        gpu_cmd = (
            f'compute ssh {worker} --zone={zone} '
            f"--command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'"
        )
        gpu_info = self.cli.gcloud(gpu_cmd)
        # sometimes the output of the command may include SSH warning messages.
        # match only lines in with expression in the following format "15109 MiB"
        match_arr = re.findall("(\d+)\s+(MiB)", gpu_info, flags=re.MULTILINE)
        num_gpus = len(match_arr)
        gpu_mem = 0
        if num_gpus == 0:
            self.cli.fail_action(ValueError(f"Unrecognized GPU memory output format: {gpu_info}"))
        for (mem_size, mem_unit) in match_arr:
            gpu_mem = max(int(mem_size), gpu_mem)
        return num_gpus, gpu_mem

    def get_worker_cpu_info(self) -> (str, str):
        return self.__get_cpu_info_for_node("worker")

    def get_master_cpu_info(self) -> (str, str):
        return self.__get_cpu_info_for_node("master")

    def get_spark_properties(self):
        software_props = self.get_value('config', 'softwareConfig', 'properties')
        return dict(software_props)

    def get_temp_gs_storage(self):
        temp_bucket = self.get_value('config', 'tempBucket')
        temp_gs = 'gs://{}/{}'.format(temp_bucket, self.uuid)
        return temp_gs

    def get_default_hs_dir(self):
        default_logdir = '{}/spark-job-history'.format(self.get_temp_gs_storage())
        # check if PHS is configured for that cluster
        phs_dir = self.get_value_silent('config',
                                        'softwareConfig',
                                        'properties',
                                        'spark:spark.eventLog.dir')
        if phs_dir:
            return [phs_dir, default_logdir]
        return [default_logdir]

    def write_as_yaml_file(self, file_path: str):
        with open(file_path, 'w') as file_path:
            yaml.dump(self.props, file_path, sort_keys=False)

    def convert_props_to_dict(self) -> dict:
        def filter_spark_properties(original_dict):
            new_dict = dict()
            for (key, value) in original_dict.items():
                # Check if key is even then add pair to new dictionary
                if key.startswith("spark:"):
                    new_dict[key.replace("spark:", '', 1)] = value
            return new_dict

        num_gpus, gpu_mem = self.get_worker_gpu_info()
        # test
        num_cpus, cpu_mem = self.get_worker_cpu_info()
        cluster_info = {
            'system': {
                'numCores': num_cpus,
                # todo: the scala code expects a unit
                'memory': f'{cpu_mem}MiB',
                'numWorkers': self.workers_count
            },
            'gpu': {
                # todo: the scala code expects a unit
                'memory': f'{gpu_mem}MiB',
                'count': num_gpus,
                'name': self.get_worker_gpu_device()
            },
            'softwareProperties': filter_spark_properties(self.get_spark_properties())
        }
        return cluster_info

