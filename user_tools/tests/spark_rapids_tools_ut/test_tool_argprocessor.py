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
import warnings
from collections import defaultdict
from typing import Dict, Callable, List

import pytest  # pylint: disable=import-error

from spark_rapids_tools import CspEnv
from spark_rapids_tools.cmdli.argprocessor import AbsToolUserArgModel, ArgValueCase
from spark_rapids_tools.enums import QualFilterApp
from .conftest import SparkRapidsToolsUT, autotuner_prop_path, all_cpu_cluster_props, all_csps


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

    @staticmethod
    def create_tool_args_should_pass(tool_name: str, platform=None, cluster=None, eventlogs=None):
        return AbsToolUserArgModel.create_tool_args(tool_name,
                                                    platform=platform,
                                                    cluster=cluster,
                                                    eventlogs=eventlogs)

    @staticmethod
    def create_tool_args_should_fail(tool_name: str, platform=None, cluster=None, eventlogs=None):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name,
                                                 platform=platform,
                                                 cluster=cluster,
                                                 eventlogs=eventlogs)
        assert pytest_wrapped_e.type == SystemExit

    @staticmethod
    def validate_tool_args(tool_name: str, tool_args: dict, cost_savings_enabled, expected_platform):
        assert tool_args['runtimePlatform'] == CspEnv(expected_platform)
        if cost_savings_enabled:
            TestToolArgProcessor.validate_args_w_savings_enabled(tool_name, tool_args)
        else:
            TestToolArgProcessor.validate_args_w_savings_disabled(tool_name, tool_args)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED])
    def test_with_platform(self, tool_name, csp):
        # should fail: platform provided; cannot run with platform only
        self.create_tool_args_should_fail(tool_name, platform=csp)

        # should fail: platform not provided; cannot run with no args
        self.create_tool_args_should_fail(tool_name=tool_name)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A])
    def test_with_platform_with_eventlogs(self, get_ut_data_dir, tool_name, csp):
        # should pass: platform and event logs are provided
        tool_args = self.create_tool_args_should_pass(tool_name,
                                                      platform=csp,
                                                      eventlogs=f'{get_ut_data_dir}/eventlogs')
        # for qualification, cost savings should be disabled because cluster is not provided
        self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                cost_savings_enabled=False,
                                expected_platform=csp)

        # should pass: platform not provided; event logs are provided
        tool_args = self.create_tool_args_should_pass(tool_name,
                                                      eventlogs=f'{get_ut_data_dir}/eventlogs')
        # for qualification, cost savings should be disabled because cluster is not provided
        self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                cost_savings_enabled=False,
                                expected_platform=CspEnv.ONPREM)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    def test_with_platform_with_cluster_name_with_eventlogs(self, get_ut_data_dir, tool_name, csp):
        if CspEnv(csp) != CspEnv.ONPREM:
            # should pass: platform, cluster name and eventlogs are provided
            tool_args = self.create_tool_args_should_pass(tool_name,
                                                          platform=csp,
                                                          cluster='my_cluster',
                                                          eventlogs=f'{get_ut_data_dir}/eventlogs')
            # for qualification, cost savings should be enabled because cluster is provided
            self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                    cost_savings_enabled=True,
                                    expected_platform=csp)

            # should pass: event logs not provided; missing eventlogs should be accepted for
            # all CSPs (except onPrem) because the event logs can be retrieved from the cluster
            tool_args = self.create_tool_args_should_pass(tool_name,
                                                          platform=csp,
                                                          cluster='my_cluster')
            # for qualification, cost savings should be enabled because cluster is provided
            self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                    cost_savings_enabled=True,
                                    expected_platform=csp)
        else:
            # should fail: platform, cluster name and eventlogs are provided; onprem platform
            # cannot run when the cluster is by name
            self.create_tool_args_should_fail(tool_name,
                                              platform=csp,
                                              cluster='my_cluster',
                                              eventlogs=f'{get_ut_data_dir}/eventlogs')

            # should fail: event logs not provided; onprem platform cannot run when the cluster is by name
            self.create_tool_args_should_fail(tool_name,
                                              platform=csp,
                                              cluster='my_cluster')

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED])
    def test_with_cluster_name_with_eventlogs(self, get_ut_data_dir, tool_name):
        # should fail: eventlogs provided; defaults platform to onprem, cannot run when the cluster is by name
        self.create_tool_args_should_fail(tool_name,
                                          cluster='my_cluster',
                                          eventlogs=f'{get_ut_data_dir}/eventlogs')

        # should fail: eventlogs not provided; defaults platform to onprem, cannot run when the cluster is by name
        self.create_tool_args_should_fail(tool_name,
                                          cluster='my_cluster')

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED])
    def test_with_platform_with_cluster_props(self, get_ut_data_dir, tool_name, csp, prop_path):
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        if CspEnv(csp) != CspEnv.ONPREM:
            # should pass: platform provided; missing eventlogs should be accepted for all CSPs (except onPrem)
            # because the eventlogs can be retrieved from the cluster properties
            tool_args = self.create_tool_args_should_pass(tool_name,
                                                          platform=csp,
                                                          cluster=cluster_prop_file)
            # for qualification, cost savings should be enabled because cluster is provided
            self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                    cost_savings_enabled=True,
                                    expected_platform=csp)

            # should pass: platform not provided; missing eventlogs should be accepted for all CSPs (except onPrem)
            # because the eventlogs can be retrieved from the cluster properties
            tool_args = self.create_tool_args_should_pass(tool_name,
                                                          cluster=cluster_prop_file)
            # for qualification, cost savings should be enabled because cluster is provided
            self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                    cost_savings_enabled=True,
                                    expected_platform=csp)
        else:
            # should fail: onprem platform cannot retrieve eventlogs from cluster properties
            self.create_tool_args_should_fail(tool_name,
                                              platform=csp,
                                              cluster=cluster_prop_file)

            # should fail: platform not provided; defaults platform to onprem, cannot retrieve eventlogs from
            # cluster properties
            self.create_tool_args_should_fail(tool_name,
                                              cluster=cluster_prop_file)

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling'])
    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.VALUE_A])
    def test_with_platform_with_cluster_props_with_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        # should pass: platform, cluster properties and eventlogs are provided
        cluster_prop_file = f'{get_ut_data_dir}/{prop_path}'
        tool_args = self.create_tool_args_should_pass(tool_name,
                                                      platform=csp,
                                                      cluster=cluster_prop_file,
                                                      eventlogs=f'{get_ut_data_dir}/eventlogs')
        # for qualification, cost savings should be enabled because cluster is provided (except for onprem)
        self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                cost_savings_enabled=CspEnv(csp) != CspEnv.ONPREM,
                                expected_platform=csp)

        # should pass: platform not provided; cluster properties and eventlogs are provided
        tool_args = self.create_tool_args_should_pass(tool_name,
                                                      cluster=cluster_prop_file,
                                                      eventlogs=f'{get_ut_data_dir}/eventlogs')
        # for qualification, cost savings should be enabled because cluster is provided (except for onprem)
        self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                cost_savings_enabled=CspEnv(csp) != CspEnv.ONPREM,
                                expected_platform=csp)

    @pytest.mark.parametrize('tool_name', ['profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @pytest.mark.parametrize('prop_path', [autotuner_prop_path])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED])
    def test_with_platform_with_autotuner(self, get_ut_data_dir, tool_name, csp, prop_path):
        # should fail: platform provided; autotuner needs eventlogs
        autotuner_prop_file = f'{get_ut_data_dir}/{prop_path}'
        self.create_tool_args_should_fail(tool_name,
                                          platform=csp,
                                          cluster=autotuner_prop_file)

        # should fail: platform not provided; autotuner needs eventlogs
        self.create_tool_args_should_fail(tool_name,
                                          cluster=autotuner_prop_file)

    @pytest.mark.parametrize('tool_name', ['profiling'])
    @pytest.mark.parametrize('csp', all_csps)
    @pytest.mark.parametrize('prop_path', [autotuner_prop_path])
    @register_triplet_test([ArgValueCase.VALUE_A, ArgValueCase.VALUE_C, ArgValueCase.VALUE_A])
    @register_triplet_test([ArgValueCase.UNDEFINED, ArgValueCase.VALUE_C, ArgValueCase.VALUE_A])
    def test_with_platform_with_autotuner_with_eventlogs(self, get_ut_data_dir, tool_name, csp, prop_path):
        # should pass: platform, autotuner properties and eventlogs are provided
        autotuner_prop_file = f'{get_ut_data_dir}/{prop_path}'
        tool_args = self.create_tool_args_should_pass(tool_name,
                                                      platform=csp,
                                                      cluster=autotuner_prop_file,
                                                      eventlogs=f'{get_ut_data_dir}/eventlogs')
        # cost savings should be disabled for profiling
        self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                cost_savings_enabled=False,
                                expected_platform=csp)

        # should pass: platform not provided; autotuner properties and eventlogs are provided
        tool_args = self.create_tool_args_should_pass(tool_name,
                                                      cluster=autotuner_prop_file,
                                                      eventlogs=f'{get_ut_data_dir}/eventlogs')
        # cost savings should be disabled for profiling
        self.validate_tool_args(tool_name=tool_name, tool_args=tool_args,
                                cost_savings_enabled=False,
                                expected_platform=CspEnv.ONPREM)

    def test_arg_cases_coverage(self):
        """
        This test ensures that above tests have covered all possible states of the `platform`, `cluster`,
        and `event logs` fields.

        Possible States:
        - platform:`undefined` or `actual value`.
        - cluster: `undefined`, `cluster name`, `cluster property file` or `auto tuner file`.
        - event logs: `undefined` or `actual value`.
        """
        arg_platform_cases = [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A]
        arg_cluster_cases = [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.VALUE_C]
        arg_eventlogs_cases = [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A]

        all_args_keys = [str([p, c, e]) for p in arg_platform_cases for c in arg_cluster_cases for e in
                         arg_eventlogs_cases]
        args_covered = set(triplet_test_registry.keys())
        args_not_covered = set(all_args_keys) - args_covered

        if args_not_covered:
            # cases not covered
            args_not_covered_str = '\n'.join(args_not_covered)
            warnings.warn(f'Cases not covered:\n{args_not_covered_str}')
            warnings.warn(f'Coverage of all argument cases: {len(args_covered)}/{len(all_args_keys)}')
