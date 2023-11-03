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
import warnings

from spark_rapids_tools import CspEnv
from spark_rapids_tools.cmdli.argprocessor import AbsToolUserArgModel, ArgValueCase, user_arg_validation_registry
from spark_rapids_tools.enums import QualFilterApp
from .conftest import SparkRapidsToolsUT, autotuner_prop_path, all_cpu_cluster_props, all_csps, csp_cpu_cluster_props, csps


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

    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    def test_with_no_args(self, tool_name):
        # should fail: cannot run with no args
        fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name)
        assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling', 'bootstrap'])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.IGNORE, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    def test_with_cluster_name(self, tool_name):
        # should fail: cannot run with cluster name only
        fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name, cluster='mycluster')
        assert pytest_wrapped_e.type == SystemExit
    
    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    def test_with_platform(self, tool_name, csp):
        # should fail: cannot run with platform only
        with pytest.raises(SystemExit) as pytest_exit_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 platform=csp)
        assert pytest_exit_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.IGNORE])
    def test_with_cluster_props_with_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        # should pass: cluster properties and eventlogs are provided 
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
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A])
    def test_with_eventlogs(self, get_ut_data_dir, tool_name):
        # should pass: eventlogs are provided
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv.ONPREM
        # for qualification, cost savings should be disabled
        self.validate_args_w_savings_disabled(tool_name, tool_args)

    
    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.IGNORE])
    def test_with_platform_with_eventlogs(self, get_ut_data_dir, tool_name, csp):
        # should pass: platform and eventlogs are provided
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         platform=csp,
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        # for qualification, cost savings should be disabled
        self.validate_args_w_savings_disabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.IGNORE, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.IGNORE, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_A, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.IGNORE, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.IGNORE, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A])
    def test_with_platform_with_cluster_name_with_eventlogs(self, get_ut_data_dir, tool_name, csp):
        if CspEnv(csp) != CspEnv.ONPREM:
            # should pass: platform, cluster name and eventlogs are provided
            tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         platform=csp,
                                                         cluster='my_cluster',
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
            assert tool_args['runtimePlatform'] == CspEnv(csp)
            self.validate_args_w_savings_enabled(tool_name, tool_args)
        else:
            # should fail: onprem platform cannot run when the cluster is by name
            with pytest.raises(SystemExit) as pytest_exit_e:
                AbsToolUserArgModel.create_tool_args(tool_name,
                                                     platform=csp,
                                                     cluster='my_cluster',
                                                     eventlogs=f'{get_ut_data_dir}/eventlogs')
            assert pytest_exit_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.IGNORE, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.IGNORE, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A])
    def test_with_cluster_name_with_eventlogs(self, get_ut_data_dir, tool_name):
        # should fail: defaults to onprem, cannot run when the cluster is by name
        with pytest.raises(SystemExit) as pytest_exit_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 cluster='my_cluster',
                                                 eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert pytest_exit_e.type == SystemExit
    

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.IGNORE, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.IGNORE, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    def test_with_platform_with_cluster_name(self, tool_name, csp):
        if CspEnv(csp) != CspEnv.ONPREM: 
            # should pass: missing eventlogs should be accepted for all CSPs (except onPrem) because
            # the eventlogs can be retrieved from the cluster
            tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                             platform=csp,
                                                             cluster='my_cluster')
            assert tool_args['runtimePlatform'] == CspEnv(csp)
            self.validate_args_w_savings_enabled(tool_name, tool_args)
        else:
            # should fail: onprem platform needs eventlogs
            with pytest.raises(SystemExit) as pytest_wrapped_e:
                AbsToolUserArgModel.create_tool_args(tool_name,
                                                     platform=csp,
                                                     cluster='my_cluster')
            assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED])
    def test_with_cluster_props(self, get_ut_data_dir, tool_name, csp, prop_path):
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        if CspEnv(csp) != CspEnv.ONPREM: 
            # should pass: missing eventlogs should be accepted for all CSPs (except onPrem) because
            # the eventlogs can be retrieved from the cluster
            tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                             cluster=f'{cluster_prop_file}')
            assert tool_args['runtimePlatform'] == CspEnv(csp)
            self.validate_args_w_savings_enabled(tool_name, tool_args)
        else:
            # should fail: defaults to onprem, cannot retrieve eventlogs from cluster properties
            with pytest.raises(SystemExit) as pytest_wrapped_e:
                AbsToolUserArgModel.create_tool_args(tool_name,
                                                     cluster=f'{cluster_prop_file}')
            assert pytest_wrapped_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED])
    def test_with_platform_with_cluster_props(self, get_ut_data_dir, tool_name, csp, prop_path):
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        if CspEnv(csp) != CspEnv.ONPREM: 
            # should pass: missing eventlogs should be accepted for all CSPs (except onPrem) because
            # the eventlogs can be retrieved from the cluster
            tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                             platform=csp,
                                                             cluster=f'{cluster_prop_file}')
            assert tool_args['runtimePlatform'] == CspEnv(csp)
            self.validate_args_w_savings_enabled(tool_name, tool_args)
        else:
            # should fail: onprem platform cannot retrieve eventlogs from cluster properties
            with pytest.raises(SystemExit) as pytest_wrapped_e:
                AbsToolUserArgModel.create_tool_args(tool_name,
                                                     platform=csp,
                                                     cluster=f'{cluster_prop_file}')
            assert pytest_wrapped_e.type == SystemExit
    
    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', csp_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_B, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_B, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.VALUE_A])
    def test_with_platform_with_cluster_props_with_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        # should pass: platform, cluster properties and eventlogs are provided
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         platform=csp,
                                                         cluster=f'{cluster_prop_file}',
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        self.validate_args_w_savings_enabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['profiling'])
    @pytest.mark.parametrize('autotuner_prop_path', [autotuner_prop_path])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED])
    def test_with_autotuner(self, get_ut_data_dir, tool_name, autotuner_prop_path):
        # should fail: autotuner needs eventlogs
        autotuner_prop_file = f'{get_ut_data_dir}/{autotuner_prop_path}'
        with pytest.raises(SystemExit) as pytest_exit_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 cluster=f'{autotuner_prop_file}')
        assert pytest_exit_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['profiling'])
    @pytest.mark.parametrize('autotuner_prop_path', [autotuner_prop_path])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_C, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_C, ArgValueCase.VALUE_A])
    def test_with_autotuner_with_eventlogs(self, get_ut_data_dir, tool_name, autotuner_prop_path):
        autotuner_prop_file = f'{get_ut_data_dir}/{autotuner_prop_path}'
        # should pass: missing eventlogs should be accepted for all CSPs (except onPrem) because
        # the eventlogs can be retrieved from the cluster
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                         cluster=f'{autotuner_prop_file}',
                                                         eventlogs=f'{get_ut_data_dir}/eventlogs')
        self.validate_args_w_savings_enabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @pytest.mark.parametrize('autotuner_prop_path', [autotuner_prop_path])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED])
    def test_with_platform_with_autotuner(self, get_ut_data_dir, tool_name, csp, autotuner_prop_path):
        # should fail: autotuner needs eventlogs
        autotuner_prop_file = f'{get_ut_data_dir}/{autotuner_prop_path}'
        with pytest.raises(SystemExit) as pytest_exit_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                platform=csp,
                                                cluster=f'{autotuner_prop_file}')
        assert pytest_exit_e.type == SystemExit

    @pytest.mark.parametrize('tool_name', ['profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @pytest.mark.parametrize('autotuner_prop_path', [autotuner_prop_path])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_C, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.IGNORE, ArgValueCase.VALUE_C, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_C, ArgValueCase.IGNORE])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_C, ArgValueCase.VALUE_A])
    def test_with_platform_with_autotuner_with_eventlogs(self, get_ut_data_dir, tool_name, csp, autotuner_prop_path):
        # should pass: platform, autotuner properties and eventlogs are provided
        autotuner_prop_file = f'{get_ut_data_dir}/{autotuner_prop_path}'
        tool_args = AbsToolUserArgModel.create_tool_args(tool_name,
                                                        platform=csp,
                                                        cluster=f'{autotuner_prop_file}',
                                                        eventlogs=f'{get_ut_data_dir}/eventlogs')
        assert tool_args['runtimePlatform'] == CspEnv(csp)
        self.validate_args_w_savings_enabled(tool_name, tool_args)

    def test_arg_cases_coverage(self):
        '''
        This test is to make sure that all argument cases are covered by unit tests. Ensure that a new
        unit test is added for each new argument case.
        '''
        arg_platform_cases = [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.IGNORE]
        arg_cluster_cases = [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.VALUE_C, ArgValueCase.IGNORE]
        arg_eventlogs_cases = [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.IGNORE]

        all_args_keys = [str([p, c, e]) for p in arg_platform_cases for c in arg_cluster_cases for e in arg_eventlogs_cases]
        args_covered = set(triplet_test_registry.keys())
        args_not_covered = set(all_args_keys) - args_covered

        if args_not_covered:
            # cases not covered
            args_not_covered_str = '\n'.join(args_not_covered)
            warnings.warn(f'Cases not covered:\n{args_not_covered_str}')
            coverage_percentage = (len(args_covered) / len(all_args_keys)) * 100
            warnings.warn(f'Coverage of all argument cases: {len(args_covered)}/{len(all_args_keys)} ({coverage_percentage:.2f}%)')
