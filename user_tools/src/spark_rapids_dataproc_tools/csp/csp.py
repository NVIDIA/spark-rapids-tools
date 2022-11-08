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
"""CSP base class."""

import logging

from spark_rapids_dataproc_tools.utilities import run_cmd

logger = logging.getLogger('csp.cspbase')


class CspBase():
    """Base class for CSP object."""

    @classmethod
    def is_csp(cls, csp_name):
        """Test CSP class by name."""
        return csp_name == 'cspbase'

    @classmethod
    def run_local_cmd(cls, cmd, check=True, capture=''):
        """Run command and capture output."""
        return run_cmd(cmd, check, capture)

    def __init__(self):
        """Init method."""

    def get_nodes(self, node='all'):
        """Get cluster node address."""
        raise NotImplementedError

    def run_ssh_cmd(self, cmd, node, check=True, capture=''):
        """Run command on cluster node via ssh and check return code, capture output etc."""
        raise NotImplementedError

    def run_scp_cmd(self, src, dest, node):
        """Run scp command to copy file to cluster node."""
        raise NotImplementedError

    def submit_job(self, job):
        """Submit job to the cluster."""
        raise NotImplementedError
