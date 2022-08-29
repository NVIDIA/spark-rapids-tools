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
"""CSP object for Dataproc."""

import logging
import os

import yaml

from .csp import CspBase

logger = logging.getLogger('csp.dataproc')


class Dataproc(CspBase):
    """Class for Dataproc."""
    JOB_TYPE_PYTHON = 'python'
    JOB_TYPE_NOTEBOOK = 'notebook'
    JOB_TYPE_SPARK = 'spark'
    JOB_TYPE_PYSPARK = 'pyspark'

    @classmethod
    def is_csp(cls, csp_name):
        """Test CSP class by name."""
        return csp_name == 'dataproc'

    def __init__(self, args):
        """Init method (required cluster & region in args)."""
        super().__init__()

        name = args.get('cluster', None)
        region = args.get('region', None)

        if not name or not region:
            raise Exception('Invalid cluster or region for Dataproc')

        self.name = name
        self.region = region
        self.nodes = {}
        self.zone = None

    def get_info(self):
        """Get cluster info."""
        result = self.run_local_cmd(['gcloud', 'dataproc', 'clusters', 'describe', self.name,
                                     f'--region={self.region}'], capture='stdout')
        return yaml.safe_load(result)

    def get_nodes(self, node='all'):
        """Get cluster node address."""
        # node format: <all|master|workers|workers-n>
        if not self.nodes:
            info = self.get_info()

            master = info.get('config', {}).get('masterConfig', {}).get('instanceNames')
            if master:
                self.nodes['master'] = master
            else:
                raise Exception("not found 'masterConfig' from cluster info")

            workers = info.get('config', {}).get('workerConfig', {}).get('instanceNames')
            if workers and len(workers) > 0:
                self.nodes['workers'] = workers
            else:
                raise Exception("sorry, single node cluster (1 master, 0 workers) not supported")

            zone_uri = info.get('config', {}).get('gceClusterConfig', {}).get('zoneUri')
            if zone_uri:
                self.zone = os.path.basename(zone_uri)
            else:
                raise Exception("not found 'zoneUri' from cluster info")

        logger.debug(f'cluster nodes: {self.nodes} from zone: {self.zone}')

        if not node or node == 'master':
            # Return master node by default
            return self.nodes['master']

        if node == 'all':
            # Return both master & worker nodes
            nodes = []
            for i in self.nodes.values():
                nodes += i
            return nodes

        if node == 'workers':
            # Return worker nodes
            return self.nodes['workers']

        # Node format: workers-n
        node_type, index_str = node.split('-')

        nodes = self.nodes.get(node_type)
        if not nodes or int(index_str) >= len(nodes):
            raise Exception(f"not found node: '{node}'")

        return [nodes[int(index_str)]]

    def run_ssh_cmd(self, cmd, node, check=True, capture=''):
        """Run command on cluster node via ssh and check return code, capture output etc."""
        ssh_cmd = ['gcloud', 'compute', 'ssh', '--zone', self.zone, node, '--']
        ssh_cmd.append(' '.join(['\'' + e + '\'' for e in cmd]))

        return self.run_local_cmd(ssh_cmd, check, capture)

    def run_scp_cmd(self, src, dest, node):
        """Run scp command to copy file to cluster node."""
        return self.run_local_cmd(['gcloud', 'compute', 'scp', '--zone', self.zone, src, f'{node}:{dest}'])

    def submit_job(self, job):
        """Submit job to the cluster."""
        cmd = ['gcloud', 'dataproc', 'jobs', 'submit', job['type'], f'--cluster={self.name}',
               f'--region={self.region}']

        # Add job class
        if 'class' in job:
            cmd += ['--class', job['class']]

        # Add job jars and properties
        if 'jars' in job:
            jars = job['jars']

            if jars:
                cmd += ['--jars', ','.join(jars)]

        if 'properties' in job:
            properties = job['properties']

            if properties:
                prop_items = []
                for key, value in properties.items():
                    prop_items.append(f"{key}='{value}'")

                cmd += ['--properties', ','.join(prop_items)]

        # Add job file
        if 'file' in job:
            cmd.append(job['file'])

        # Add job parameters
        if 'parameters' in job and job['parameters']:
            cmd += ['--'] + job['parameters']

        # Capture stderr as job output to stderr
        return self.run_local_cmd(cmd, capture='stderr')
