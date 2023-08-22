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

"""Test Tool argument validators"""


import fire
import pytest  # pylint: disable=import-error

from as_pytools import CspEnv
from as_pytools.cmdli.argprocessor import AbsToolUserArgModel
from as_pytools.enums import QualFilterApp
from as_pytools.exceptions import IllegalArgumentError
from .conftest import AsCliUnitTest, all_cpu_cluster_props


class TestToolArgProcessor(AsCliUnitTest):  # pylint: disable=too-few-public-methods
    """
    Class testing toolArgProcessor functionalities
    """

    @staticmethod
    def validate_args_w_savings_enabled(t_args: dict):
        assert t_args['savingsCalculations']
        # filterApps should be set to savings
        assert t_args['filterApps'] == QualFilterApp.SAVINGS

    @staticmethod
    def validate_args_w_savings_disabled(t_args: dict):
        assert not t_args['savingsCalculations']
        # filterApps should be set to savings
        assert t_args['filterApps'] == QualFilterApp.SPEEDUPS

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling', 'bootstrap'])
    def test_no_args(self, tool_name):
        fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name)
        assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    def test_with_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         cluster=f'{cluster_prop_file}',
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        if tool_name == 'qualification':
            # for qualification, passing the cluster properties should be enabled unless it is
            # onprem platform that requires target_platform
            if CspEnv(csp) != CspEnv.ONPREM:
                self.validate_args_w_savings_enabled(tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    def test_no_cluster_props(self, get_ut_data_dir, tool_name):
        # all eventlogs are stored on local path. there is no way to find which cluster
        # we refer to.
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv.ONPREM
        if tool_name == 'qualification':
            # for qualification, cost savings should be disabled
            self.validate_args_w_savings_disabled(tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    def test_onprem_disallow_cluster_by_name(self, get_ut_data_dir, tool_name):
        # onprem platform cannot run when the cluster is by_name
        with pytest.raises(IllegalArgumentError) as ex_arg:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 cluster='my_cluster',
                                                 eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert 'Invalid arguments: Cannot run cluster by name with platform [onprem]' in str(ex_arg.value)
