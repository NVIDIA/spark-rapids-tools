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

"""Implementation class representing wrapper around diagnostic tool."""

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

    def requires_cluster_connection(self) -> bool:
        return True

    def _connect_to_execution_cluster(self):
        super()._connect_to_execution_cluster()

        self.exec_cluster = self.get_exec_cluster()
        self.all_nodes = self.exec_cluster.get_all_nodes()

    def _process_output_args(self):
        super()._process_output_args()

        # Set remote output folder same as local output folder name
        output_path = self.ctxt.get_output_folder()
        folder_name = FSUtil.get_resource_name(output_path)
        self.ctxt.set_remote('outputFolder', folder_name)

    def _upload_scripts(self):
        """
        Upload scripts to both driver & worker nodes
        :return:
        """
        script = Utils.resource_path('collect.sh')

        try:
            for node in self.all_nodes:
                self.logger.info('Uploading script to node: %s', node.get_name())
                self.exec_cluster.scp_to_node(node, str(script), '/tmp/')

        except Exception as e:
            self.logger.error('Error while uploading script to remote node')
            raise e

    def _run_rapids_tool(self):
        """
        Run diagnostic tool from both driver & worker nodes to collect info
        :return:
        """
        self.logger.info('Uploading script to remote cluster nodes:')
        self._upload_scripts()

        self.logger.info('Collecting info on remote cluster nodes:')
        remote_output_folder = self.ctxt.get_remote('outputFolder')
        ssh_cmd = f'"PREFIX={remote_output_folder} /tmp/collect.sh"'

        try:
            for node in self.all_nodes:
                self.logger.info('Collecting info on node: %s', node.get_name())
                self.exec_cluster.run_cmd_node(node, ssh_cmd)

        except Exception as e:
            self.logger.error('Error while collecting info from remote node')
            raise e

    def _download_output(self):
        self.logger.info('Downloading results from remote nodes:')

        output_path = self.ctxt.get_output_folder()
        remote_output_folder = self.ctxt.get_remote('outputFolder')
        remote_output_result = f'/tmp/{remote_output_folder}*.tgz'

        try:
            for node in self.all_nodes:
                node_output_path = FSUtil.build_path(output_path, node.get_name())
                FSUtil.make_dirs(node_output_path, exist_ok=True)

                self.logger.info('Downloading results from node: %s', node.get_name())
                self.exec_cluster.scp_from_node(node, remote_output_result, node_output_path)

        except Exception as e:
            self.logger.error('Error while downloading collected info from remote node')
            raise e

    def _process_output(self):
        self.logger.info('Processing the collected results.')

        output_path = self.ctxt.get_output_folder()
        region = self.exec_cluster.get_region()
        worker_count = self.exec_cluster.get_nodes_cnt(SparkNodeType.WORKER)

        master_type = self.exec_cluster.get_node_instance_type(SparkNodeType.MASTER)
        worker_type = self.exec_cluster.get_node_instance_type(SparkNodeType.WORKER)

        # Cleanup unused work dir
        work_dir = FSUtil.build_path(output_path, self.ctxt.get_local_work_dir())
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
        output_path = self.ctxt.get_output_folder()
        Utils.make_archive(output_path, 'tar', output_path)
        self.logger.info("Archive '%s.tar' is successfully created.", output_path)

    def _finalize(self):
        pass
