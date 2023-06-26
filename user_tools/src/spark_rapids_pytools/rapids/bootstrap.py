# Copyright (c) 2023, NVIDIA CORPORATION.
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

"""Implementation class representing wrapper around the RAPIDS acceleration Bootstrap tool."""

from dataclasses import dataclass

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase, NodeHWInfo
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_tool import RapidsTool


@dataclass
class Bootstrap(RapidsTool):
    """
    Wrapper layer around Bootstrap Tool.
    """
    name = 'bootstrap'

    def _process_custom_args(self):
        dry_run_opt = self.wrapper_options.get('dryRun', 'False')
        self.ctxt.set_ctxt('dryRunOpt', bool(dry_run_opt))

    def requires_cluster_connection(self) -> bool:
        return True

    def __calculate_spark_settings(self, worker_info: NodeHWInfo) -> dict:
        """
        Calculate the cluster properties that we need to append to the /etc/defaults of the spark
        if necessary.
        :param worker_info: the hardware info as extracted from the worker. Note that we assume
                            that all the workers have the same configurations.
        :return: dictionary containing 7 spark properties to be set by default on the cluster.
        """
        num_gpus = worker_info.gpu_info.num_gpus
        gpu_mem = worker_info.gpu_info.gpu_mem
        num_cpus = worker_info.sys_info.num_cpus
        cpu_mem = worker_info.sys_info.cpu_mem

        constants = self.ctxt.get_value('local', 'clusterConfigs', 'constants')
        executors_per_node = num_gpus
        num_executor_cores = max(1, num_cpus // executors_per_node)
        gpu_concurrent_tasks = min(constants.get('maxGpuConcurrent'), gpu_mem // constants.get('gpuMemPerTaskMB'))
        # account for system overhead
        usable_worker_mem = max(0, cpu_mem - constants.get('systemReserveMB'))
        executor_container_mem = usable_worker_mem // executors_per_node
        # reserve 10% of heap as memory overhead
        max_executor_heap = max(0, int(executor_container_mem * (1 - constants.get('heapOverheadFraction'))))
        # give up to 2GB of heap to each executor core
        executor_heap = min(max_executor_heap, constants.get('heapPerCoreMB') * num_executor_cores)
        executor_mem_overhead = int(executor_heap * constants.get('heapOverheadFraction'))
        # use default for pageable_pool to add to memory overhead
        pageable_pool = constants.get('defaultPageablePoolMB')
        # pinned memory uses any unused space up to 4GB
        pinned_mem = min(constants.get('maxPinnedMemoryMB'),
                         executor_container_mem - executor_heap - executor_mem_overhead - pageable_pool)
        executor_mem_overhead += pinned_mem + pageable_pool
        res = {
            'spark.executor.cores': num_executor_cores,
            'spark.executor.memory': f'{executor_heap}m',
            'spark.executor.memoryOverhead': f'{executor_mem_overhead}m',
            'spark.rapids.sql.concurrentGpuTasks': gpu_concurrent_tasks,
            'spark.rapids.memory.pinnedPool.size': f'{pinned_mem}m',
            'spark.sql.files.maxPartitionBytes': f'{constants.get("maxSqlFilesPartitionsMB")}m',
            'spark.task.resource.gpu.amount': 1 / num_executor_cores,
            'spark.rapids.shuffle.multiThreaded.reader.threads': num_executor_cores,
            'spark.rapids.shuffle.multiThreaded.writer.threads': num_executor_cores,
            'spark.rapids.sql.multiThreadedRead.numThreads': max(20, num_executor_cores)
        }
        return res

    def _run_rapids_tool(self):
        """
        Run the bootstrap on the driver node
        :return:
        """
        self.logger.info('Executing Bootstrap commands on remote cluster to calculate default configurations.')
        exec_cluster: ClusterBase = self.get_exec_cluster()
        worker_hw_info = exec_cluster.get_worker_hw_info()
        self.logger.debug('Worker hardware INFO %s', worker_hw_info)
        try:
            spark_settings = self.__calculate_spark_settings(worker_info=worker_hw_info)
            self.ctxt.set_ctxt('bootstrap_results', spark_settings)
            self.logger.debug('%s Tool finished calculating recommended Apache Spark configurations for cluster %s: %s',
                              self.pretty_name(),
                              self.cluster,
                              str(spark_settings))
        except Exception as e:
            self.logger.error('Error while calculating spark configurations')
            raise e

    def _apply_changes_to_remote_cluster(self):
        ssh_cmd = "\"sudo bash -c 'cat >> /etc/spark/conf/spark-defaults.conf'\""
        cmd_input = self.ctxt.get_ctxt('wrapperOutputContent')
        exec_cluster = self.get_exec_cluster()
        try:
            exec_cluster.run_cmd_driver(ssh_cmd, cmd_input=cmd_input)
        except RuntimeError as re:
            self.logger.warning('An exception was raised while applying the '
                                'recommendation to the cluster: %s', re)

    def _process_output(self):
        self.logger.info('Processing the result of Spark properties')
        tool_result = self.ctxt.get_ctxt('bootstrap_results')
        exec_cluster = self.get_exec_cluster()
        dry_run = self.ctxt.get_ctxt('dryRunOpt')
        if tool_result is not None and any(tool_result):
            # write the result to log file
            # Now create the new folder
            FSUtil.make_dirs(self.ctxt.get_output_folder(), exist_ok=True)
            wrapper_out_content_arr = [f'##### BEGIN : RAPIDS bootstrap settings for {exec_cluster.name}']
            for conf_key, conf_val in tool_result.items():
                wrapper_out_content_arr.append(f'{conf_key}={conf_val}')
            wrapper_out_content_arr.append(f'##### END : RAPIDS bootstrap settings for {exec_cluster.name}\n')
            shuffle_manager_note = 'Note: to turn on the Spark RAPIDS multithreaded shuffle, you will also\n' \
                                   'have to enable this setting based on the Spark version of your cluster:\n' \
                                   'spark.shuffle.manager=com.nvidia.spark.rapids.spark3xx.RapidShuffleManager.\n'
            wrapper_out_content_arr.append(shuffle_manager_note)
            wrapper_out_content = Utils.gen_multiline_str(wrapper_out_content_arr)
            self.ctxt.set_ctxt('wrapperOutputContent', wrapper_out_content)
            if dry_run:
                self.logger.info('Skipping applying configurations to remote cluster %s. DRY_RUN is enabled.',
                                 exec_cluster.name)
            else:
                # apply the changes to remote cluster
                try:
                    self._apply_changes_to_remote_cluster()
                except RuntimeError as err:
                    self.logger.error('Error applying changes to driver node on cluster %s.', exec_cluster.name)
                    raise err
            # write the result to log file
            out_file_path = self.ctxt.get_wrapper_summary_file_path()
            self.logger.info('Saving configuration to local file %s', out_file_path)
            with open(out_file_path, 'w', encoding='utf-8') as wrapper_output:
                wrapper_output.write(wrapper_out_content)
        else:
            # results are empty
            self.ctxt.set_ctxt('wrapperOutputContent',
                               self._report_results_are_empty())

    def _delete_remote_dep_folder(self):
        self.logger.debug('%s mode skipping deleting the remote workdir', self.pretty_name())

    def _download_remote_output_folder(self):
        self.logger.debug('%s skipping downloading the remote output workdir', self.pretty_name())

    def _report_tool_full_location(self) -> str:
        out_file_path = self.ctxt.get_wrapper_summary_file_path()
        return Utils.gen_multiline_str(f'{self.pretty_name()} tool output: {out_file_path}')

    def _write_summary(self):
        wrapper_out_content = self.ctxt.get_ctxt('wrapperOutputContent')
        print(Utils.gen_multiline_str(self._report_tool_full_location(),
                                      'Recommended Configurations:',
                                      wrapper_out_content))
