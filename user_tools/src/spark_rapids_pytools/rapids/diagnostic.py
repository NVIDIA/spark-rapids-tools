# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

"""Implementation class representing wrapper around diagnostic tool."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase, SparkNodeType
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_tool import RapidsTool


@dataclass
class Diagnostic(RapidsTool):
    """
    Wrapper layer around Diagnostic Tool.
    """
    name = 'diagnostic'
    exec_cluster: ClusterBase = None
    all_nodes: list = None
    thread_num: int = 3

    def _process_custom_args(self):
        thread_num = self.wrapper_options.get('threadNum', 3)
        if thread_num < 1 or thread_num > 10:
            raise RuntimeError(f'Invalid thread number: {thread_num} (Valid value: 1~10)')

        self.thread_num = thread_num
        self.logger.debug('Set thread number as: %d', self.thread_num)
        log_message = ('This operation will collect sensitive information from your cluster, '
                       'such as OS & HW info, Yarn/Spark configurations and log files etc.')
        yes = self.wrapper_options.get('yes', False)
        if yes:
            self.logger.warning(log_message)
            self.logger.info('Confirmed by command line option.')
        else:
            # Pause the spinner for user prompt
            self.spinner.pause(insert_newline=True)
            print(log_message)
            user_input = input('Do you want to continue (yes/no): ')
            if user_input.lower() not in ['yes', 'y']:
                raise RuntimeError('User canceled the operation.')
            self.spinner.resume()

    def requires_cluster_connection(self) -> bool:
        return True

    def _connect_to_execution_cluster(self):
        super()._connect_to_execution_cluster()

        self.exec_cluster = self.get_exec_cluster()
        self.all_nodes = self.exec_cluster.get_all_nodes()

    def _process_output_args(self):
        super()._process_output_args()

        # Set remote output folder same as local output folder name
        output_path = self.ctxt.get_csp_output_path()
        folder_name = FSUtil.get_resource_name(output_path)
        self.ctxt.set_remote('outputFolder', folder_name)

    def _upload_scripts(self, node):
        """
        Upload scripts to specified node
        :return:
        """
        script = Utils.resource_path('collect.sh')

        try:
            self.logger.info('Uploading script to node: %s', node.get_name())
            self.exec_cluster.scp_to_node(node, str(script), '/tmp/')

        except Exception as e:
            self.logger.error('Error while uploading script to node: %s', node.get_name())
            raise e

    def _collect_info(self, node):
        """
        Run task to collect info from specified node
        :return:
        """
        self._upload_scripts(node)

        remote_output_folder = self.ctxt.get_remote('outputFolder')
        ssh_cmd = f'"PREFIX={remote_output_folder} PLATFORM_TYPE={self.platform_type} /tmp/collect.sh"'

        try:
            self.logger.info('Collecting info on node: %s', node.get_name())
            self.exec_cluster.run_cmd_node(node, ssh_cmd)

        except Exception as e:
            self.logger.error('Error while collecting info from node: %s', node.get_name())
            raise e

    def _run_rapids_tool(self):
        """
        Run diagnostic tool from both driver & worker nodes to collect info
        :return:
        """
        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            for e in executor.map(self._collect_info, self.all_nodes):
                # Raise exception if any error occurred
                if e:
                    raise e

    def _download_output(self):
        self.logger.info('Downloading results from remote nodes:')

        output_path = self.ctxt.get_csp_output_path()
        remote_output_folder = self.ctxt.get_remote('outputFolder')
        remote_output_result = f'/tmp/{remote_output_folder}*.tgz'

        def _download_result(node):
            try:
                node_output_path = FSUtil.build_path(output_path, node.get_name())
                FSUtil.make_dirs(node_output_path, exist_ok=True)

                self.logger.info('Downloading results from node: %s', node.get_name())
                self.exec_cluster.scp_from_node(node, remote_output_result, node_output_path)

            except Exception as e:
                self.logger.error('Error while downloading collected info from node: %s', node.get_name())
                raise e

        with ThreadPoolExecutor(max_workers=self.thread_num) as executor:
            for e in executor.map(_download_result, self.all_nodes):
                # Raise exception if any error occurred
                if e:
                    raise e

    def _process_output(self):
        self.logger.info('Processing the collected results.')

        output_path = self.ctxt.get_csp_output_path()
        region = self.exec_cluster.get_region()
        worker_count = self.exec_cluster.get_nodes_cnt(SparkNodeType.WORKER)

        master_type = self.exec_cluster.get_node_instance_type(SparkNodeType.MASTER)
        worker_type = self.exec_cluster.get_node_instance_type(SparkNodeType.WORKER)

        # Cleanup unused work dir
        work_dir = FSUtil.build_path(output_path, self.ctxt.get_local_work_dir())
        # TODO - would be nice to have option to not remove this directory
        # until then the below remove_path can be commented out during debugging
        FSUtil.remove_path(work_dir, fail_ok=True)

        # Save cluster info
        self.logger.info('Saving cluster info.')
        output_file = FSUtil.build_path(output_path, 'cluster.info')
        with open(output_file, 'w', encoding='UTF-8') as f:
            f.write(f'Region: {region}\n')
            f.write(f'Worker count: {worker_count}\n')
            f.write(f'Master type: {master_type}\n')
            f.write(f'Worker type: {worker_type}\n')

    def _archive_results(self):
        output_path = self.ctxt.get_csp_output_path()
        Utils.make_archive(output_path, 'tar', output_path)
        self.logger.info("Archive '%s.tar' is successfully created.", output_path)

    def _finalize(self):
        pass
