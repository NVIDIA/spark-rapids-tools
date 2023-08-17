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

"""Implementation of argument processors for Tools"""

import dataclasses
from enum import IntEnum
from functools import partial
from logging import Logger
from typing import Optional, Any, ClassVar

from pydantic import model_validator, ValidationError
from pydantic.dataclasses import dataclass
from pydantic_core import PydanticCustomError

from as_pytools.cloud import ClientCluster
from as_pytools.exceptions import IllegalArgumentError
from as_pytools.utils import AbstractPropContainer, is_http_file
from spark_rapids_pytools.cloud_api.sp_types import DeployMode
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.qualification import QualGpuClusterReshapeType
from ..enums import QualFilterApp, CspEnv
from ..storagelib.csppath import CspPath
from ..tools.autotuner import AutoTunerPropMgr


class ArgValueCase(IntEnum):
    """
    Enum cases representing the status of each argument. Used to decide on the case of each
    argument to decide whether the input is valid or not.
    """
    UNDEFINED = 1
    VALUE_A = 2
    VALUE_B = 4
    VALUE_C = 8
    IGNORE = 15

    @classmethod
    def are_equal(cls, value1: 'ArgValueCase', value2: 'ArgValueCase') -> bool:
        return (value1 & value2) != 0

    @classmethod
    def array_equal(cls, arr1: list, arr2: list) -> bool:
        if len(arr1) != len(arr2):
            return False
        return all(cls.are_equal(arr1[i], arr2[i]) for i in range(len(arr1)))


@dataclass
class AbstractToolUserArgModel:
    """
    Abstract class that represent the arguments collected by the user to run the tools.
    This is used as doing preliminary validation against some of the common pattern
    """
    cluster: Optional[str] = None
    platform: Optional[CspEnv] = None
    output_folder: Optional[str] = None
    rejected: dict = dataclasses.field(init=False, default_factory=dict)
    detected: dict = dataclasses.field(init=False, default_factory=dict)
    extra: dict = dataclasses.field(init=False, default_factory=dict)
    argv_cases: list = dataclasses.field(init=False,
                                         default_factory=lambda: [])
    p_args: dict = dataclasses.field(init=False, default_factory=lambda: {
        'meta': {},
        'toolArgs': {}
    })
    logger: ClassVar[Logger] = ToolLogging.get_and_setup_logger('ascli.argparser')

    @classmethod
    def create_tool_args(cls, *args: Any, **kwargs: Any) -> Optional[dict]:
        try:
            new_obj = object.__new__(cls)
            cls.__init__(new_obj, *args, **kwargs)
            return new_obj.build_tools_args()
        except ValidationError as e:
            cls.logger.error('Validation err: %s', e)
        return None

    def get_eventlogs(self) -> Optional[str]:
        if hasattr(self, 'eventlogs'):
            return self.eventlogs
        return None

    def raise_validation_exception(self, validation_err: str):
        raise IllegalArgumentError(
            f'Invalid arguments: {validation_err}')

    def determine_cluster_arg_type(self) -> ArgValueCase:
        # self.cluster is provided. then we need to verify that the expected files are there
        if CspPath.is_file_path(self.cluster, raise_on_error=False):
            # check it is valid prop file
            if AbstractPropContainer.is_valid_prop_path(self.cluster, raise_on_error=False):
                # the file cannot be a http_url
                if is_http_file(self.cluster):
                    # we do not accept http://urls
                    raise IllegalArgumentError(
                        f'Cluster properties cannot be a web URL path: {self.cluster}')
                cluster_case = ArgValueCase.VALUE_B
            else:
                raise PydanticCustomError(
                    'file_path',
                    'Cluster property file is not in valid format {.json, .yaml, or .yml}')
        else:
            cluster_case = ArgValueCase.VALUE_A
        return cluster_case

    def detect_platform_from_cluster_prop(self):
        client_cluster = ClientCluster(CspPath(self.cluster))
        self.p_args['toolArgs']['platform'] = CspEnv.fromstring(client_cluster.platform_name)

    def detect_platform_from_eventlogs_prefix(self):
        map_storage_to_platform = {
            'gcs': CspEnv.DATAPROC,
            's3': CspEnv.EMR,
            'local': CspEnv.ONPREM,
            'hdfs': CspEnv.ONPREM,
            'adls': CspEnv.DATABRICKS_AZURE
        }
        # in case we have a list of eventlogs, we need to split them and take the first one
        ev_logs_path = CspPath(self.get_eventlogs().split(',')[0])
        storage_type = ev_logs_path.get_storage_name()
        self.p_args['toolArgs']['platform'] = map_storage_to_platform[storage_type]

    def validate_onprem_with_cluster_name(self):
        if self.platform == CspEnv.ONPREM:
            raise IllegalArgumentError(
                f'Invalid arguments: Cannot run cluster by name with platform [{CspEnv.ONPREM}]')

    def init_extra_arg_cases(self) -> list:
        return []

    def init_tool_args(self):
        pass

    def init_arg_cases(self):
        if self.platform is None:
            self.argv_cases.append(ArgValueCase.UNDEFINED)
        else:
            self.argv_cases.append(ArgValueCase.VALUE_A)
        if self.cluster is None:
            self.argv_cases.append(ArgValueCase.UNDEFINED)
        else:
            self.argv_cases.append(self.determine_cluster_arg_type())
        self.argv_cases.extend(self.init_extra_arg_cases())

    def define_invalid_arg_cases(self):
        pass

    def define_detection_cases(self):
        pass

    def define_extra_arg_cases(self):
        pass

    def build_tools_args(self) -> dict:
        pass

    def apply_arg_cases(self):
        for curr_cases in [self.rejected, self.detected, self.extra]:
            for case_key, case_value in curr_cases.items():
                if any(ArgValueCase.array_equal(self.argv_cases, case_i) for case_i in case_value['cases']):
                    # debug the case key
                    self.logger.info('...applying argument case: %s', case_key)
                    case_value['callable']()

    def validate_arguments(self):
        self.init_tool_args()
        self.init_arg_cases()
        self.define_invalid_arg_cases()
        self.define_detection_cases()
        self.define_extra_arg_cases()
        self.apply_arg_cases()


@dataclass
class ToolUserArgModel(AbstractToolUserArgModel):
    """
    Abstract class that represents the arguments collected by the user to run the tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    eventlogs: Optional[str] = None

    def init_extra_arg_cases(self) -> list:
        if self.eventlogs is None:
            return [ArgValueCase.UNDEFINED]
        return [ArgValueCase.VALUE_A]

    def define_invalid_arg_cases(self):
        super().define_invalid_arg_cases()
        self.rejected['Missing Eventlogs'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd. Cannot define eventlogs from input '
                                '(platform, cluster, and eventlogs)'),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED]
            ]
        }
        self.rejected['Cluster By Name Without Platform Hints'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd on a named cluster without hints about the target '
                                'platform. Re-run the command providing at least one of the '
                                'eventlogs/platform arguments.'),
            'cases': [
                [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.UNDEFINED]
            ]
        }
        self.rejected['Cluster By Name Cannot go with OnPrem'] = {
            'valid': False,
            'callable': partial(self.validate_onprem_with_cluster_name),
            'cases': [
                [ArgValueCase.VALUE_A, ArgValueCase.VALUE_A, ArgValueCase.IGNORE]
            ]
        }

    def define_detection_cases(self):
        self.detected['Define Platform from Cluster Properties file'] = {
            'valid': True,
            'callable': partial(self.detect_platform_from_cluster_prop),
            'cases': [
                [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_B, ArgValueCase.IGNORE]
            ]
        }
        self.detected['Define Platform based on Eventlogs prefix'] = {
            'valid': True,
            'callable': partial(self.detect_platform_from_eventlogs_prefix),
            'cases': [
                [ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A],
                [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A, ArgValueCase.VALUE_A]
            ]
        }


@dataclass
class QualifyUserArgModel(ToolUserArgModel):
    """
    Represents the arguments collected by the user to run the qualification tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    target_platform: Optional[CspEnv] = None
    filter_apps: Optional[QualFilterApp] = None
    gpu_cluster_recommendation: Optional[QualGpuClusterReshapeType] = None

    def init_tool_args(self):
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['savingsCalculations'] = True
        self.p_args['toolArgs']['filterApps'] = self.filter_apps
        self.p_args['toolArgs']['targetPlatform'] = self.target_platform

    def define_extra_arg_cases(self):
        self.extra['Disable CostSavings'] = {
            'valid': True,
            'callable': partial(self.disable_savings_calculations),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.VALUE_A]
            ]
        }

    def _reset_savings_flags(self, reason_msg: Optional[str] = None):
        self.p_args['toolArgs']['savingsCalculations'] = False
        if self.p_args['toolArgs']['filterApps'] == QualFilterApp.SAVINGS:
            # we cannot use QualFilterApp.SAVINGS if savingsCalculations is disabled.
            self.p_args['toolArgs']['filterApps'] = QualFilterApp.SPEEDUPS
        if reason_msg:
            self.logger.info('Cost saving is disabled: %s', reason_msg)

    def disable_savings_calculations(self):
        self._reset_savings_flags(reason_msg='Cluster\'s information is missing.')
        self.p_args['toolArgs']['targetPlatform'] = None

    @model_validator(mode='after')
    def validate_arg_cases(self) -> 'QualifyUserArgModel':
        # shortcircuit to fail early
        self.validate_arguments()
        return self

    def build_tools_args(self) -> dict:
        # At this point, if the platform is still none, then we can set it to the default value
        # which is the onPrem platform.
        if self.p_args['toolArgs']['platform'] is None:
            # set the platform to default onPrem
            runtime_platform = CspEnv.get_default()
        else:
            runtime_platform = self.p_args['toolArgs']['platform']
        # check the targetPlatform argument
        if self.p_args['toolArgs']['targetPlatform']:
            equivalent_pricing_list = runtime_platform.get_equivalent_pricing_platform()
            if not equivalent_pricing_list:
                # no target_platform for that runtime environment
                self.logger.info(
                    'Argument target_platform does not support the current cluster [%s]', runtime_platform)
                self.p_args['toolArgs']['targetPlatform'] = None
            else:
                if not self.p_args['toolArgs']['targetPlatform'] in equivalent_pricing_list:
                    raise IllegalArgumentError(
                        'Invalid arguments: '
                        f'The platform [{self.p_args["toolArgs"]["targetPlatform"]}] is currently '
                        f'not supported to calculate savings from [{runtime_platform}] cluster')
        else:
            # target platform is not set, then we disable cost savings if the runtime platform if
            # onprem
            if CspEnv.requires_pricing_map(runtime_platform):
                self._reset_savings_flags(reason_msg=f'Platform [{runtime_platform}] requires '
                                                     '"target_platform" argument to generate cost savings')

        # check the filter_apps argument
        if self.p_args['toolArgs']['filterApps'] is None:
            # set a default filterApps argument to be savings if the cost savings is enabled
            if self.p_args['toolArgs']['savingsCalculations']:
                self.p_args['toolArgs']['filterApps'] = QualFilterApp.SAVINGS
            else:
                self.p_args['toolArgs']['filterApps'] = QualFilterApp.SPEEDUPS

        # finally generate the final values
        wrapped_args = {
            'runtimePlatform': runtime_platform,
            'outputFolder': self.output_folder,
            'platformOpts': {
                'credentialFile': None,
                'deployMode': DeployMode.LOCAL,
                # used to be sent to the scala core java cmd
                'targetPlatform': self.p_args['toolArgs']['targetPlatform']
            },
            'migrationClustersProps': {
                'cpuCluster': self.cluster,
                'gpuCluster': None
            },
            'jobSubmissionProps': {
                'remoteFolder': None,
                'platformArgs': {
                    'jvmMaxHeapSize': 24
                }
            },
            'eventlogs': self.eventlogs,
            'filterApps': QualFilterApp.fromstring(self.p_args['toolArgs']['filterApps']),
            'toolsJar': None,
            'gpuClusterRecommendation': QualGpuClusterReshapeType.fromstring(self.gpu_cluster_recommendation),
            # used to initialize the pricing information
            'targetPlatform': self.p_args['toolArgs']['targetPlatform']
        }
        return wrapped_args


@dataclass
class ProfileUserArgModel(ToolUserArgModel):
    """
    Represents the arguments collected by the user to run the profiling tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    def determine_cluster_arg_type(self) -> ArgValueCase:
        cluster_case = super().determine_cluster_arg_type()
        if cluster_case == ArgValueCase.VALUE_B:
            # determine is this an autotuner file or not
            auto_tuner_prop_obj = AutoTunerPropMgr.load_from_file(self.cluster, raise_on_error=False)
            if auto_tuner_prop_obj:
                cluster_case = ArgValueCase.VALUE_C
                self.p_args['toolArgs']['autotuner'] = self.cluster
        return cluster_case

    def init_tool_args(self):
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['autotuner'] = None

    def define_invalid_arg_cases(self):
        super().define_invalid_arg_cases()
        self.rejected['Autotuner requires eventlogs'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd. AutoTuner requires eventlogs argument'),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED]
            ]
        }

    def define_detection_cases(self):
        super().define_detection_cases()
        # append the case when the autotuner input
        self.detected['Define Platform based on Eventlogs prefix']['cases'].append(
            [ArgValueCase.UNDEFINED, ArgValueCase.VALUE_C, ArgValueCase.VALUE_A]
        )

    @model_validator(mode='after')
    def validate_arg_cases(self) -> 'ProfileUserArgModel':
        # shortcircuit to fail early
        self.validate_arguments()
        return self

    def build_tools_args(self) -> dict:
        if self.p_args['toolArgs']['platform'] is None:
            # set the platform to default onPrem
            runtime_platform = CspEnv.get_default()
        else:
            runtime_platform = self.p_args['toolArgs']['platform']
        # check if the cluster infor was autotuner_input
        if self.p_args['toolArgs']['autotuner']:
            # this is an autotuner input
            self.p_args['toolArgs']['cluster'] = None
        else:
            # this is an actual cluster argument
            self.p_args['toolArgs']['cluster'] = self.cluster
        # finally generate the final values
        wrapped_args = {
            'runtimePlatform': runtime_platform,
            'outputFolder': self.output_folder,
            'platformOpts': {
                'credentialFile': None,
                'deployMode': DeployMode.LOCAL,
            },
            'migrationClustersProps': {
                'gpuCluster': self.p_args['toolArgs']['cluster']
            },
            'jobSubmissionProps': {
                'remoteFolder': None,
                'platformArgs': {
                    'jvmMaxHeapSize': 24
                }
            },
            'eventlogs': self.eventlogs,
            'toolsJar': None,
            'autoTunerFileInput': self.p_args['toolArgs']['autotuner']
        }

        return wrapped_args


@dataclass
class BootstrapUserArgModel(AbstractToolUserArgModel):
    dry_run: Optional[bool] = True

    def build_tools_args(self) -> dict:
        return {
            'runtimePlatform': self.platform,
            'outputFolder': self.output_folder,
            'platformOpts': {},
            'dryRun': self.dry_run
        }
