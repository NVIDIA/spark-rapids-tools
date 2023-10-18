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

import dataclasses
from collections import defaultdict
from typing import Dict, Callable, List

import fire
import pytest  # pylint: disable=import-error

from spark_rapids_tools import CspEnv
from spark_rapids_tools.cmdli.argprocessor import AbsToolUserArgModel, ArgValueCase
from spark_rapids_tools.enums import QualFilterApp
from .conftest import SparkRapidsToolsUT, all_cpu_cluster_props, csp_cpu_cluster_props, csps


@dataclasses.dataclass
class TripletArgCase:
    argv_cases: List[ArgValueCase]
    label: str = dataclasses.field(init=False)
    tests: List[str] = dataclasses.field(init=False, default_factory=lambda: [])

    def __post_init__(self):
        self.label = str(self.argv_cases)


# We will use this lookup table to check against coverage of all argument cases
# The way this can be done is to write one last unit-test that loop on all the dictionary and make
# sure that all entries have unit tests
triplet_test_registry: Dict[str, TripletArgCase] = defaultdict(TripletArgCase)


def register_triplet_test(argv_cases: list):
    def decorator(func_cb: Callable):
        obj_k = str(argv_cases)
        argv_obj = triplet_test_registry.get(obj_k)
        if argv_obj is None:
            argv_obj = TripletArgCase(argv_cases)
            triplet_test_registry[obj_k] = argv_obj
        argv_obj.tests.append(func_cb.__name__)
        return func_cb
    return decorator


class TestToolArgProcessor(SparkRapidsToolsUT):  # pylint: disable=too-few-public-methods
    """
    Class testing toolArgProcessor functionalities
    """

    @staticmethod
    def validate_args_w_savings_enabled(tool_name: str, t_args: dict):
        if tool_name == 'qualification':
            assert t_args['savingsCalculations']
            # filterApps should be set to savings
            assert t_args['filterApps'] == QualFilterApp.SAVINGS

    @staticmethod
    def validate_args_w_savings_disabled(tool_name: str, t_args: dict):
        if tool_name == 'qualification':
            assert not t_args['savingsCalculations']
            # filterApps should be set to savings
            assert t_args['filterApps'] == QualFilterApp.SPEEDUPS

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling', 'bootstrap'])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    def test_no_args(self, tool_name):
        fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name)
        assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling', 'bootstrap'])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    def test_cluster__name_no_hints(self, tool_name):
        fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name, cluster='mycluster')
        assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.VALUE_A])
    def test_with_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         cluster=f'{cluster_prop_file}',
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        # for qualification, passing the cluster properties should be enabled unless it is
        # onprem platform that requires target_platform
        if CspEnv(csp) != CspEnv.ONPREM:
            self.validate_args_w_savings_enabled(tool_name, tool_args)
        else:
            self.validate_args_w_savings_disabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A])
    def test_no_cluster_props(self, get_ut_data_dir, tool_name):
        # all eventlogs are stored on local path. There is no way to find which cluster
        # we refer to.
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv.ONPREM
        # for qualification, cost savings should be disabled
        self.validate_args_w_savings_disabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.IGNORE])
    def test_onprem_disallow_cluster_by_name(self, get_ut_data_dir, tool_name):
        # onprem platform cannot run when the cluster is by_name
        with pytest.raises(SystemExit) as pytest_exit_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 cluster='my_cluster',
                                                 eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert pytest_exit_e.type == SystemExit
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 platform='onprem',
                                                 cluster='my_cluster')
        assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', csps)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    def test_cluster_name_no_eventlogs(self, tool_name, csp):
        # Missing eventlogs should be accepted for all CSPs (except onPrem)
        # because the eventlogs can be retrieved from the cluster
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         platform=csp,
                                                         cluster='my_cluster')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        self.validate_args_w_savings_enabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', csp_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED])
    def test_cluster_props_no_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        # Missing eventlogs should be accepted for all CSPs (except onPrem)
        # because the eventlogs can be retrieved from the cluster
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         cluster=f'{cluster_prop_file}')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        self.validate_args_w_savings_enabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    def test_cluster_props_no_eventlogs_on_prem(self, capsys, tool_name):
        # Missing eventlogs is not accepted for onPrem
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 platform='onprem')
        assert pytest_wrapped_e.type == SystemExit
        captured = capsys.readouterr()
        # Verify there is no URL in error message
        assert 'https://' not in captured.err

    @pytest.mark.skip(reason='Unit tests are not completed yet')
    def test_arg_cases_coverage(self):
        args_keys = [
            [ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED],
            [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED],
            [ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.IGNORE],
            [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.IGNORE],
            [ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A],
            [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A],
            [ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A]
        ]

        for arg_key in args_keys:
            assert str(arg_key) in triplet_test_registry
