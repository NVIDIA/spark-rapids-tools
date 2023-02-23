# Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

"""Utilities and helpers defined to interface with dataproc."""

import os
import re
import subprocess
from dataclasses import dataclass, field
from logging import Logger
from typing import Optional, Tuple, Callable

import yaml
from tabulate import tabulate

from spark_rapids_dataproc_tools.utilities import is_system_tool, bail, YAMLPropertiesContainer, \
    convert_dict_to_camel_case, get_gpu_short_name

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
                raise ValueError(f'Tool {tool} is not in installed or not in the PATH environment')
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
    return normalized_mc.startswith('n1-')


def get_incompatible_criteria(**criteria_container) -> dict:
    """
    This function checks whether a node can host the RAPIDS plugin.
    For example, for dataproc. image versions 1.5 runs Spark 2.x which cannot run the plugin.
    :return: a dictionary containing the key-value pairs for criteria that were not satisfied
    """
    incompatible = {}
    comments = {}
    if 'imageVersion' in criteria_container:
        image_ver = criteria_container.get('imageVersion')
        if image_ver.startswith('1.5'):
            incompatible['imageVersion'] = '2.0+'
            comments['imageVersion'] = (
                f'The cluster image {image_ver} is not supported. To support the RAPIDS user tools, '
                f'you will need to use an image that runs Spark3.x.'
            )
    if 'machineType' in criteria_container:
        machine_type = criteria_container.get('machineType')
        if not is_machine_compatible_for_gpu(machine_type):
            converted_type = map_to_closest_supported_match(machine_type)
            incompatible['machineType'] = converted_type
            comments['machineType'] = (
                f'To support acceleration with T4 GPUs, you will need to switch '
                f'your worker node instance type <{machine_type}> to {converted_type}.'
            )
    if 'workerLocalSSDs' in criteria_container:
        local_ssds = criteria_container.get('workerLocalSSDs')
        if local_ssds == 0:
            incompatible['workerLocalSSDs'] = 1
            comments['workerLocalSSDs'] = (
                'Worker nodes have no local SSDs. '
                'Local SSD is recommended for Spark scratch space to improve IO.'
            )
    if len(comments) > 0:
        incompatible['comments'] = comments
    return incompatible


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
    instance_name_parts = machine_type.split('-')
    # The last bit of the instance name is the number of cores
    original_core = instance_name_parts[-1]
    return f'n1-{instance_name_parts[1]}-{get_core(int(original_core))}'


def default_gpu_device_memory(machine_type: str, gpu_device: str) -> int:
    """
    returns the default memory in MiB for the given GPU device.
    :param gpu_device: short name of the gpu_device
    :param machine_type: the machine type of the node. This is used to distinguish high GPU nodes
                        like a2-ultragpu-* in dataproc
    :return: memory size in MB for a single GPU of the given device.
    """
    memory_sizes = {
        # source https://cloud.google.com/compute/docs/gpus#nvidia_gpus_for_compute_workloads
        # default for A100 is 40GB per GPU (a2-highgpu).
        # It is possible to have 80 GB for machines a2-ultragpu
        'A100': 40960,
        # N1 machine series supports 16 GB per GPU
        'T4': 16384,
        # N1 machine series supports 8 GB per GPU
        'P4': 8192,
        # N1 machine series supports 16 GB per GPU
        'P100': 16384,
        # N1 machine series supports 12 GB per GPU
        'K80': 12288
    }
    gpu_memory_in_mib = memory_sizes.get(gpu_device)
    if gpu_device == 'A100' and machine_type.lower().find('ultragpu') != -1:
        return gpu_memory_in_mib * 2
    return gpu_memory_in_mib


@dataclass
class CMDRunner:
    """
    Class implementation that encapsulates command executions.
    """
    logger: Logger
    debug: bool
    fail_action_cb: Callable[[Exception, Optional[str]], int] = None

    def _check_subprocess_result(self, c, expected, msg_fail: str):
        try:
            if c.returncode != expected:
                stderror_content = c.stderr.decode('utf-8')
                std_error_lines = [f'\t| {line}' for line in stderror_content.splitlines()]
                stderr_str = ''
                if len(std_error_lines) > 0:
                    error_lines = '\n'.join(std_error_lines)
                    stderr_str = f'\n{error_lines}'
                cmd_err_msg = f'Error invoking CMD <{c.args}>: {stderr_str}'
                raise RuntimeError(f'{cmd_err_msg}')
        except RuntimeError as ex:
            if self.fail_action_cb is None:
                if msg_fail:
                    bail(f'{msg_fail}', ex)
                bail('Error invoking cmd', ex)
            self.fail_action_cb(ex, msg_fail)

    def run(self, cmd: str, expected: int = 0, fail_ok: bool = False,
            msg_fail: Optional[str] = None, cmd_ip=None):
        """
        Internal implementation to execute commands and capture the results.
        :param cmd:
        :param expected:
        :param fail_ok:
        :param msg_fail:
        :param cmd_ip: Allowing you to pass bytes or a string to the subprocess's stdin.
        :return: the std_out resulting from running a given command.
        """
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE
        # pylint: disable=subprocess-run-check
        # We do not want to check the result here, because the caller is responsible for ignoring
        # the error or not.
        if cmd_ip is None:
            c = subprocess.run(cmd, shell=True, stdout=stdout, stderr=stderr)
        else:
            c = subprocess.run(cmd, shell=True, stdout=stdout, stderr=stderr, input=cmd_ip)
        # pylint: enable=subprocess-run-check
        if not fail_ok:
            self._check_subprocess_result(c, expected=expected, msg_fail=msg_fail)
        std_output = c.stdout.decode('utf-8')
        std_error = c.stderr.decode('utf-8')
        if self.debug:
            # reformat lines to make the log more readable
            std_output_lines = [f'\t| {line}' for line in std_output.splitlines()]
            std_error_lines = [f'\t| {line}' for line in std_error.splitlines()]
            stdout_str = ''
            stderr_str = ''
            if len(std_output_lines) > 0:
                std_out_lines = '\n'.join(std_output_lines)
                stdout_str = f'\n\t<STDOUT>\n{std_out_lines}'
            if len(std_error_lines) > 0:
                std_err_lines = '\n'.join(std_error_lines)
                stderr_str = f'\n\t<STDERR>\n{std_err_lines}'
            self.logger.debug('executing CMD:\n\t<CMD: %s>[%s]; [%s]', cmd, stdout_str, stderr_str)
        return std_output

    def gcloud(self, cmd, expected: int = 0, msg_fail: Optional[str] = None,
               fail_ok: bool = False,
               cmd_input=None):
        gcloud_cmd = f'gcloud {cmd}'
        return self.run(gcloud_cmd, expected=expected, msg_fail=msg_fail, fail_ok=fail_ok, cmd_ip=cmd_input)

    def gcloud_submit_as_spark(self, cmd_args, err_msg):
        return self.gcloud(f'dataproc jobs submit spark {cmd_args}', msg_fail=err_msg)

    def gcloud_describe_cluster(self, cluster: str, region: str, fail_msg):
        describe_cmd = f'dataproc clusters describe {cluster} --region={region}'
        return self.gcloud(describe_cmd, msg_fail=fail_msg)

    def gcloud_ssh(self,
                   node: str,
                   zone: str,
                   cmd: str,
                   expected: int = 0,
                   msg_fail: Optional[str] = None,
                   fail_ok: bool = False,
                   cmd_input=None):
        ssh_command = (
            f'compute ssh {node} --zone={zone} '
            f"--command='{cmd}'"
        )
        return self.gcloud(cmd=ssh_command, expected=expected, msg_fail=msg_fail, fail_ok=fail_ok,
                           cmd_input=cmd_input)

    def gsutil(self, cmd, expected: int = 0, msg_fail: Optional[str] = None, fail_ok: bool = False):
        gsutil_cmd = f'gsutil {cmd}'
        return self.run(gsutil_cmd, expected=expected, msg_fail=msg_fail, fail_ok=fail_ok)

    def gcloud_rm(self, remote_path: str, is_dir: bool = True, fail_ok: bool = False):
        recurse = '-r' if is_dir else ''
        multi_threaded = '-m' if not is_mac else ''
        rm_cmd = f'{multi_threaded} rm -f {recurse} {remote_path}'
        self.gsutil(rm_cmd, fail_ok=fail_ok)

    def gcloud_cp(self, src_path: str,
                  dst_path: str,
                  overwrite: bool = True,
                  is_dir: bool = True,
                  fail_ok: bool = False):
        cp_cmd = (
            f'{"-m" if not is_mac else ""} cp {"" if overwrite else "-n"} {"-r" if is_dir else ""}'
            f' {src_path} {dst_path}'
        )
        self.gsutil(cp_cmd, fail_ok=fail_ok)

    def gcloud_cat(self,
                   remote_path: str,
                   msg_fail: Optional[str] = None,
                   fail_ok: bool = False) -> str:
        """
        The cat command outputs the contents of one URL to stdout.
        :param remote_path: a valid url of an existing file.
        :param msg_fail: message to display when the command fails.
        :param fail_ok: Do not fail if the file crashes.
        :return: the content of the gsutil cat command if successful.
        """
        cat_cmd = f'cat {remote_path}'
        return self.gsutil(cat_cmd, msg_fail=msg_fail, fail_ok=fail_ok)


@dataclass
class DataprocClusterPropContainer(YAMLPropertiesContainer):
    """
    Implementation of a wrapper that holds the properties of a dataproc cluster.
    """
    cli: CMDRunner = None
    uuid: str = field(default=None, init=False)
    workers_count: int = field(default=None, init=False)

    def _process_loaded_props(self) -> None:
        """
        After loading the raw properties, perform any necessary processing to clean up the
        properties.
        """

    def _set_cluster_uuid(self) -> None:
        # It is possible that the UIUD is not valid when the cluster is offline.
        self.uuid = self.get_value('clusterUuid')

    def _init_fields(self) -> None:
        self._process_loaded_props()
        self._set_cluster_uuid()
        self.workers_count = self.get_value('config', 'workerConfig', 'numInstances')
        incompatible_cluster = get_incompatible_criteria(imageVersion=self.get_image_version())
        if len(incompatible_cluster) > 0:
            comments = incompatible_cluster.get('comments')['imageVersion']
            self.cli.logger.warning('%s', comments)

    def __decode_machine_type_uri(self, uri):
        uri_parts = uri.split('/')[-4:]
        if uri_parts[0] != 'zones' or uri_parts[2] != 'machineTypes':
            self.cli.fail_action_cb(ValueError(f'Unable to parse machine type from machine type URI: {uri}'),
                                    'Failed while processing CPU info')
        zone_val = uri_parts[1]
        machine_type_val = uri_parts[3]
        return zone_val, machine_type_val

    def get_cpu_info_for_machine_type(self, zone: str, machine_type: str) -> (str, str):
        """
        This method can be used for offline clusters because it does not ssh to the cluster.
        """
        exec_cmd = f'compute machine-types describe {machine_type} --zone={zone}'
        raw_output = self.cli.gcloud(exec_cmd)
        type_config = yaml.safe_load(raw_output)
        num_cpus = type_config['guestCpus']
        cpu_mem = type_config['memoryMb']
        return num_cpus, cpu_mem

    def _get_cpu_info_for_node(self, node_type: str) -> (str, str):
        type_uri = self.get_value('config', f'{node_type}Config', 'machineTypeUri')
        zone, machine_type = self.__decode_machine_type_uri(type_uri)
        return self.get_cpu_info_for_machine_type(zone=zone, machine_type=machine_type)

    def __get_local_ssds_for_node(self, node_type: str) -> int:
        ssds_count = self.get_value_silent('config', f'{node_type}Config', 'diskConfig', 'numLocalSsds')
        if ssds_count is None:
            return 0
        return int(ssds_count)

    def __get_number_instances(self, node_type: str) -> int:
        # it is possible that the value is missing. The default is assumed to be 1.
        res = self.get_value_silent('config', f'{node_type}Config', 'numInstances')
        if res is None:
            return 1
        return int(res)

    def _get_machine_info_for_node(self, node_type: str) -> (str, str, str):
        """
        Extracts the machine type of Dataproc node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing  node region, zone, and type.
        """
        type_uri = self.get_value('config', f'{node_type}Config', 'machineTypeUri')
        zone, machine_type = self.__decode_machine_type_uri(type_uri)
        zone_parts = zone.split('-')
        region = f'{zone_parts[0]}-{zone_parts[1]}'
        return region, zone, machine_type

    def convert_worker_machine_if_not_supported(self) -> dict:
        _, _, worker_machine = self.get_worker_machine_info()
        incompatibility = get_incompatible_criteria(machineType=worker_machine)
        return incompatibility

    def check_all_incompatibilities(self) -> dict:
        _, _, worker_machine = self.get_worker_machine_info()
        return get_incompatible_criteria(machineType=worker_machine,
                                         imageVersion=self.get_image_version(),
                                         workerLocalSSDs=self.get_worker_local_ssds())

    def get_master_vm_instances(self):
        return self.__get_number_instances('master')

    def get_worker_vm_instances(self):
        return self.__get_number_instances('worker')

    def get_image_version(self):
        return self.get_value('config', 'softwareConfig', 'imageVersion')

    def get_master_local_ssds(self):
        return self.__get_local_ssds_for_node('master')

    def get_worker_local_ssds(self):
        return self.__get_local_ssds_for_node('worker')

    def get_master_machine_info(self) -> (str, str, str):
        """
        Extracts the machine type of Dataproc master node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing master node machine region, zone, and type.
        """
        return self._get_machine_info_for_node('master')

    def get_worker_machine_info(self) -> (str, str, str):
        """
        Extracts the machine type of Dataproc master node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing master node machine region, zone, and type.
        """
        return self._get_machine_info_for_node('worker')

    def get_zone(self) -> str:
        """
        Extracts the zone of a Dataproc cluster from the cluster's configuration.
        :return: cluster zone as a string
        """
        zoneuri = self.get_value('config', 'gceClusterConfig', 'zoneUri')
        return zoneuri[zoneuri.rindex('/') + 1:]

    def get_driver_sshcmd_prefix(self) -> str:
        zone = self.get_zone()
        node = self.get_value('config', 'masterConfig', 'instanceNames')[0]
        return f'compute ssh {node} --zone={zone}'

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
            self.cli.fail_action_cb(ValueError(f'Unrecognized tesla device: {gpu_devices}'),
                                    'Failed while processing GpuDevice')
        for line in all_lines:
            gpu_device = parse_supported_gpu(line)
            if gpu_device is None:
                self.cli.fail_action_cb(ValueError(f'Unrecognized tesla device: {line}'),
                                        'Failed while processing GpuDevice')
            return gpu_device

    def _pull_worker_gpu_memories(self) -> str:
        """
        Executes ssh command on worker node and retuens the output as string.
        :return: a string in the form of "15109 MiB\n15109 MiB"
        """
        zone = self.get_zone()
        worker = self.get_value('config', 'workerConfig', 'instanceNames')[0]
        gpu_driver_err_msg = (
            'Could not ssh to cluster or Cluster does not support GPU. '
            'Make sure the cluster is running and NVIDIA drivers are installed.')
        gpu_info = self.cli.gcloud_ssh(worker,
                                       zone,
                                       'nvidia-smi --query-gpu=memory.total --format=csv,noheader',
                                       msg_fail=gpu_driver_err_msg)
        return gpu_info

    def get_worker_gpu_info(self) -> (int, int):
        """
        Returns information on the GPUs assigned to each worker in a Dataproc cluster.
        :return: pair containing GPU count per worker and individual GPU memory size in megabytes
        """
        gpu_info = self._pull_worker_gpu_memories()
        # sometimes the output of the command may include SSH warning messages.
        # match only lines in with expression in the following format (15109 MiB)
        match_arr = re.findall(r'(\d+)\s+(MiB)', gpu_info, flags=re.MULTILINE)
        num_gpus = len(match_arr)
        gpu_mem = 0
        if num_gpus == 0:
            self.cli.fail_action_cb(ValueError(f'Unrecognized GPU memory output format: {gpu_info}'))
        for (mem_size, _) in match_arr:
            gpu_mem = max(int(mem_size), gpu_mem)
        return num_gpus, gpu_mem

    def get_worker_cpu_info(self) -> (str, str):
        return self._get_cpu_info_for_node('worker')

    def get_master_cpu_info(self) -> (str, str):
        return self._get_cpu_info_for_node('master')

    def get_spark_properties(self) -> dict:
        """
        return the properties of Spark properties if found in the cluster properties.
        :return: a dictionary of key, value pairs of spark properties. None, if not available
        """
        software_props = self.get_value_silent('config', 'softwareConfig', 'properties')
        if software_props is not None:
            return dict(software_props)
        return None

    def get_temp_gs_storage(self):
        temp_bucket = self.get_value('config', 'tempBucket')
        temp_gs = f'gs://{temp_bucket}/{self.uuid}'
        return temp_gs

    def get_default_hs_dir(self):
        default_logdir = f'{self.get_temp_gs_storage()}/spark-job-history'
        # check if PHS is configured for that cluster
        phs_dir = self.get_value_silent('config',
                                        'softwareConfig',
                                        'properties',
                                        'spark:spark.eventLog.dir')
        if phs_dir:
            return [phs_dir, default_logdir]
        return [default_logdir]

    def write_as_yaml_file(self, file_path: str):
        with open(file_path, 'w', encoding='utf-8') as yaml_file:
            yaml.dump(self.props, yaml_file, sort_keys=False)

    def worker_pretty_print(self, extra_args: dict = None, headers: Tuple = (), prefix='\n') -> str:
        gpu_region, gpu_zone, gpu_worker_machine = self.get_worker_machine_info()
        lines = [['Workers', self.get_worker_vm_instances()],
                 ['Worker Machine Type', gpu_worker_machine],
                 ['Region', gpu_region],
                 ['Zone', gpu_zone]]
        if extra_args is not None:
            for prop_k, prop_v in extra_args.items():
                lines.append([prop_k, prop_v])
        return f'{prefix}{tabulate(lines, headers)}'

    def convert_props_to_dict(self) -> dict:
        def filter_spark_properties(original_dict):
            new_dict = {}
            if original_dict is None:
                return new_dict
            for (key, value) in original_dict.items():
                # Check if key is even then add pair to new dictionary
                if key.startswith('spark:'):
                    new_dict[key.replace('spark:', '', 1)] = value
            return new_dict

        num_gpus, gpu_mem = self.get_worker_gpu_info()
        # test
        num_cpus, cpu_mem = self.get_worker_cpu_info()
        cluster_info = {
            'system': {
                'numCores': num_cpus,
                # the scala code expects a unit
                'memory': f'{cpu_mem}MiB',
                'numWorkers': self.workers_count
            },
            'gpu': {
                # the scala code expects a unit
                'memory': f'{gpu_mem}MiB',
                'count': num_gpus,
                'name': self.get_worker_gpu_device()
            },
            'softwareProperties': filter_spark_properties(self.get_spark_properties())
        }
        return cluster_info


@dataclass
class DataprocShadowClusterPropContainer(DataprocClusterPropContainer):
    """
    Represents an offline cluster that is not currently live.
    This implementation implies that accessing the cluster is not feasible, and that all the
    required properties should be available in the configuration file passed along to the constructor
    of the object.
    """
    def _process_loaded_props(self):
        def _dfs_configuration(curr_props) -> dict:
            for key, value in curr_props.items():
                if isinstance(value, dict):
                    if key in ('config', 'clusterConfig'):
                        return dict({'config': value})
                    returned = _dfs_configuration(value)
                    if returned is not None:
                        return returned
                # ignore, we should continue looping
            return None
        # convert the keys to camelCase
        self.props = convert_dict_to_camel_case(self.props)
        # skip values we are not interested in
        self.props = _dfs_configuration(self.props)

    def set_container_region(self, node_type: str, region: str) -> None:
        node_prefix_key = f'{node_type}Config'
        self.props['config'][node_prefix_key]['region'] = region

    def set_container_zone(self, node_type: str, zone: str) -> None:
        node_prefix_key = f'{node_type}Config'
        self.props['config'][node_prefix_key]['zone'] = zone

    def _set_cluster_uuid(self) -> None:
        """
        An offline cluster has no uuid.
        """
        self.uuid = None

    def _get_machine_info_for_node(self, node_type: str) -> (str, str, str):
        """
        Extracts the machine type of Dataproc node from the cluster's configuration.
        :return: tuple (region, zone, machine_type) containing  node region, zone, and type.
        """
        # for offline clusters , the machinetype has no url
        node_prefix_key = f'{node_type}Config'
        machine_type = self.get_value('config', node_prefix_key, 'machineTypeUri')
        zone = self.get_value('config', node_prefix_key, 'zone')
        region = self.get_value('config', node_prefix_key, 'region')
        return region, zone, machine_type

    def _get_cpu_info_for_node(self, node_type: str) -> (str, str):
        _, zone, machine_type = self._get_machine_info_for_node(node_type)
        return self.get_cpu_info_for_machine_type(zone=zone, machine_type=machine_type)

    def get_worker_gpu_info(self) -> (int, int):
        """
        Returns information on the GPUs assigned to each worker in a Dataproc cluster.
        :return: pair containing GPU count per worker and individual GPU memory size in megabytes
        """
        worker_accelerators = self.get_value_silent('config', 'workerConfig', 'accelerators')
        if worker_accelerators is None or len(worker_accelerators) == 0:
            # this is an error
            self.cli.fail_action_cb(ValueError('Unable to find worker accelerators'),
                                    'Failed while processing CPU info')
        # we have an array
        for worker_acc in worker_accelerators:
            # loop on each accelerator to see if this is a valid GPU accelerator
            count = worker_acc.get('acceleratorCount')
            acc_type = worker_acc.get('acceleratorTypeUri')
            short_gpu_name = get_gpu_short_name(acc_type)
            if short_gpu_name is not None:
                worker_machine = self.get_value('config', 'workerConfig', 'machineTypeUri')
                self.props['config']['workerConfig']['gpuShortName'] = short_gpu_name
                self.props['config']['workerConfig']['gpuCount'] = count
                # this is a valid GPU accelerator
                return count, default_gpu_device_memory(worker_machine, short_gpu_name)
        return 0, 0

    def get_worker_gpu_device(self) -> str:
        return self.get_value('config', 'workerConfig', 'gpuShortName')
