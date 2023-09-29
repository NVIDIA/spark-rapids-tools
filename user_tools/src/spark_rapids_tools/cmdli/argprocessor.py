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
from collections import defaultdict
from enum import IntEnum
from functools import partial
from logging import Logger
from typing import Optional, Any, ClassVar, Callable, Type, Dict

from pydantic import model_validator, ValidationError
from pydantic.dataclasses import dataclass
from pydantic_core import PydanticCustomError

from spark_rapids_tools.cloud import ClientCluster
from spark_rapids_tools.utils import AbstractPropContainer, is_http_file
from spark_rapids_pytools.cloud_api.sp_types import DeployMode
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.qualification import QualGpuClusterReshapeType
from ..enums import QualFilterApp, CspEnv
from ..storagelib.csppath import CspPath
from ..tools.autotuner import AutoTunerPropMgr
from ..utils.util import dump_tool_usage


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


class UserArgValidatorImpl:  # pylint: disable=too-few-public-methods
    """
    A metaclass holding information about the validator responsible to process user's input.
    """
    name: str
    _validator_class: Type['AbsToolUserArgModel']

    @property
    def validator_class(self) -> Type['AbsToolUserArgModel']:
        return self._validator_class

    @validator_class.setter
    def validator_class(self, clazz):
        self._validator_class = clazz


user_arg_validation_registry: Dict[str, UserArgValidatorImpl] = defaultdict(UserArgValidatorImpl)


def register_tool_arg_validator(tool_name: str) -> Callable:
    def decorator(cls: type) -> type:
        cls.tool_name = tool_name
        user_arg_validation_registry[tool_name].name = tool_name
        user_arg_validation_registry[tool_name].validator_class = cls
        return cls
    return decorator


@dataclass
class AbsToolUserArgModel:
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
    logger: ClassVar[Logger] = None
    tool_name: ClassVar[str] = None

    @classmethod
    def create_tool_args(cls, tool_name: str, *args: Any, **kwargs: Any) -> Optional[dict]:
        cls.logger = ToolLogging.get_and_setup_logger('spark_rapids_tools.argparser')
        try:
            impl_entry = user_arg_validation_registry.get(tool_name)
            impl_class = impl_entry.validator_class
            new_obj = impl_class(*args, **kwargs)
            return new_obj.build_tools_args()
        except (ValidationError, PydanticCustomError) as e:
            impl_class.logger.error('Validation err: %s\n', e)
            dump_tool_usage(impl_class.tool_name)
        return None

    def get_eventlogs(self) -> Optional[str]:
        if hasattr(self, 'eventlogs'):
            return self.eventlogs
        return None

    def raise_validation_exception(self, validation_err: str):
        raise PydanticCustomError(
            'invalid_argument',
            f'{validation_err}\n  Error:')

    def determine_cluster_arg_type(self) -> ArgValueCase:
        # self.cluster is provided. then we need to verify that the expected files are there
        if CspPath.is_file_path(self.cluster, raise_on_error=False):
            # check it is valid prop file
            if AbstractPropContainer.is_valid_prop_path(self.cluster, raise_on_error=False):
                # the file cannot be a http_url
                if is_http_file(self.cluster):
                    # we do not accept http://urls
                    raise PydanticCustomError(
                        'invalid_argument',
                        f'Cluster properties cannot be a web URL path: {self.cluster}\n  Error:')
                cluster_case = ArgValueCase.VALUE_B
            else:
                raise PydanticCustomError(
                    'file_path',
                    'Cluster property file is not in valid format {.json, .yaml, or .yml}\n  Error:')
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
            raise PydanticCustomError(
                'invalid_argument',
                f'Cannot run cluster by name with platform [{CspEnv.ONPREM}]\n  Error:')

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

    def get_or_set_platform(self) -> CspEnv:
        if self.p_args['toolArgs']['platform'] is None:
            # set the platform to default onPrem
            runtime_platform = CspEnv.get_default()
        else:
            runtime_platform = self.p_args['toolArgs']['platform']
        self.post_platform_assignment_validation(runtime_platform)
        return runtime_platform

    def post_platform_assignment_validation(self, assigned_platform):
        # do some validation after we decide the cluster type
        if self.argv_cases[1] == ArgValueCase.VALUE_A:
            if assigned_platform == CspEnv.ONPREM:
                # it is not allowed to run cluster_by_name on an OnPrem platform
                raise PydanticCustomError(
                    'invalid_argument',
                    f'Cannot run cluster by name with platform [{CspEnv.ONPREM}]\n  Error:')


@dataclass
class ToolUserArgModel(AbsToolUserArgModel):
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
@register_tool_arg_validator('qualification')
class QualifyUserArgModel(ToolUserArgModel):
    """
    Represents the arguments collected by the user to run the qualification tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    target_platform: Optional[CspEnv] = None
    filter_apps: Optional[QualFilterApp] = None
    gpu_cluster_recommendation: Optional[QualGpuClusterReshapeType] = None
    cpu_cluster_price: Optional[float] = None
    estimated_gpu_cluster_price: Optional[float] = None
    cpu_discount: Optional[int] = None
    gpu_discount: Optional[int] = None
    global_discount: Optional[int] = None

    def init_tool_args(self):
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['savingsCalculations'] = True
        self.p_args['toolArgs']['filterApps'] = self.filter_apps
        self.p_args['toolArgs']['targetPlatform'] = self.target_platform
        self.p_args['toolArgs']['cpuClusterPrice'] = self.cpu_cluster_price
        self.p_args['toolArgs']['estimatedGpuClusterPrice'] = self.estimated_gpu_cluster_price
        self.p_args['toolArgs']['cpuDiscount'] = self.cpu_discount
        self.p_args['toolArgs']['gpuDiscount'] = self.gpu_discount
        self.p_args['toolArgs']['globalDiscount'] = self.global_discount
        # check the reshapeType argument
        if self.gpu_cluster_recommendation is None:
            self.p_args['toolArgs']['gpuClusterRecommendation'] = QualGpuClusterReshapeType.get_default()
        else:
            self.p_args['toolArgs']['gpuClusterRecommendation'] = self.gpu_cluster_recommendation

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
        runtime_platform = self.get_or_set_platform()
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
                    raise PydanticCustomError(
                        'invalid_argument',
                        f'The platform [{self.p_args["toolArgs"]["targetPlatform"]}] is currently '
                        f'not supported to calculate savings from [{runtime_platform}] cluster\n  Error:')
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
            'savingsCalculations': self.p_args['toolArgs']['savingsCalculations'],
            'eventlogs': self.eventlogs,
            'filterApps': QualFilterApp.fromstring(self.p_args['toolArgs']['filterApps']),
            'toolsJar': None,
            'gpuClusterRecommendation': self.p_args['toolArgs']['gpuClusterRecommendation'],
            # used to initialize the pricing information
            'targetPlatform': self.p_args['toolArgs']['targetPlatform'],
            'cpuClusterPrice': self.p_args['toolArgs']['cpuClusterPrice'],
            'estimatedGpuClusterPrice': self.p_args['toolArgs']['estimatedGpuClusterPrice'],
            'cpuDiscount': self.p_args['toolArgs']['cpuDiscount'],
            'gpuDiscount': self.p_args['toolArgs']['gpuDiscount'],
            'globalDiscount': self.p_args['toolArgs']['globalDiscount']
        }
        return wrapped_args


@dataclass
@register_tool_arg_validator('profiling')
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
        runtime_platform = self.get_or_set_platform()
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
@register_tool_arg_validator('bootstrap')
class BootstrapUserArgModel(AbsToolUserArgModel):
    """
    Represents the arguments collected by the user to run the bootstrap tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    dry_run: Optional[bool] = True

    def build_tools_args(self) -> dict:
        return {
            'runtimePlatform': self.platform,
            'outputFolder': self.output_folder,
            'platformOpts': {},
            'dryRun': self.dry_run
        }

    @model_validator(mode='after')
    def validate_non_empty_args(self) -> 'BootstrapUserArgModel':
        error_flag = 0
        components = []
        if self.cluster is None:
            error_flag = 1
            components.append('cluster')
        if self.platform is None:
            error_flag |= 2
            components.append('platform')
        if error_flag > 0:
            missing = str.join(' and ', components)
            raise ValueError(f'Cmd requires [{missing}] to be specified')
        return self
