# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
from ..enums import QualFilterApp, CspEnv, QualEstimationModel
from ..storagelib.csppath import CspPath
from ..tools.autotuner import AutoTunerPropMgr
from ..utils.util import dump_tool_usage, Utilities


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
    tools_jar: Optional[str] = None
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
        # this field has already been populated during initialization
        selected_platform = self.p_args['toolArgs']['platform']
        if selected_platform == CspEnv.ONPREM:
            raise PydanticCustomError(
                'invalid_argument',
                f'Cannot run cluster by name with platform [{CspEnv.ONPREM}]\n  Error:')

    def validate_onprem_with_cluster_props_without_eventlogs(self):
        # this field has already been populated during initialization
        selected_platform = self.p_args['toolArgs']['platform']
        if selected_platform == CspEnv.ONPREM:
            raise PydanticCustomError(
                'invalid_argument',
                f'Cannot run cluster by properties with platform [{CspEnv.ONPREM}] without event logs\n  Error:')

    def validate_jar_argument_is_valid(self):
        if self.tools_jar is not None:
            if not CspPath.is_file_path(self.tools_jar,
                                        extensions=['jar'],
                                        raise_on_error=False):
                raise PydanticCustomError(
                    'file_path',
                    f'Jar file path {self.tools_jar} is not valid\n  Error:')
        self.p_args['toolArgs']['toolsJar'] = self.tools_jar

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

    def apply_arg_cases(self, cases_list: list):
        for curr_cases in cases_list:
            for case_key, case_value in curr_cases.items():
                if any(ArgValueCase.array_equal(self.argv_cases, case_i) for case_i in case_value['cases']):
                    # debug the case key
                    self.logger.info('...applying argument case: %s', case_key)
                    case_value['callable']()

    def apply_all_arg_cases(self):
        self.apply_arg_cases([self.rejected, self.detected, self.extra])

    def validate_arguments(self):
        self.init_tool_args()
        self.init_arg_cases()
        self.define_invalid_arg_cases()
        self.define_detection_cases()
        self.define_extra_arg_cases()
        self.apply_all_arg_cases()

    def get_or_set_platform(self) -> CspEnv:
        if self.p_args['toolArgs']['platform'] is None:
            # set the platform to default onPrem
            runtime_platform = CspEnv.get_default()
        else:
            runtime_platform = self.p_args['toolArgs']['platform']
        self.post_platform_assignment_validation()
        return runtime_platform

    def post_platform_assignment_validation(self):
        # Update argv_cases to reflect the platform
        self.argv_cases[0] = ArgValueCase.VALUE_A
        # Any validation post platform assignment should be done here
        self.apply_arg_cases([self.rejected, self.extra])


@dataclass
class ToolUserArgModel(AbsToolUserArgModel):
    """
    Abstract class that represents the arguments collected by the user to run the tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    eventlogs: Optional[str] = None
    jvm_heap_size: Optional[int] = None
    jvm_threads: Optional[int] = None

    def is_concurrent_submission(self):
        return False

    def process_jvm_args(self):
        # JDK8 uses parallel-GC by default. Set the GC algorithm to G1GC
        self.p_args['toolArgs']['jvmGC'] = '+UseG1GC'
        jvm_heap = self.jvm_heap_size
        if jvm_heap is None:
            # set default GC heap size based on the virtual memory of the host.
            jvm_heap = Utilities.get_system_memory_in_gb()
        # check if both tools are going to run concurrently, then we need to reduce the heap size
        # To reduce possibility of OOME, each core-tools thread should be running with at least 6 GB
        # of heap.
        adjusted_resources = Utilities.adjust_tools_resources(jvm_heap,
                                                              jvm_processes=2 if self.is_concurrent_submission() else 1,
                                                              jvm_threads=self.jvm_threads)
        self.p_args['toolArgs']['jvmMaxHeapSize'] = jvm_heap
        self.p_args['toolArgs']['jobResources'] = adjusted_resources

    def init_extra_arg_cases(self) -> list:
        if self.eventlogs is None:
            return [ArgValueCase.UNDEFINED]
        return [ArgValueCase.VALUE_A]

    def define_invalid_arg_cases(self):
        super().define_invalid_arg_cases()
        self.define_rejected_missing_eventlogs()
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
        self.rejected['Cluster By Properties Cannot go with OnPrem'] = {
            'valid': False,
            'callable': partial(self.validate_onprem_with_cluster_props_without_eventlogs),
            'cases': [
                [ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED]
            ]
        }
        self.rejected['Invalid Jar Argument'] = {
            'valid': False,
            'callable': partial(self.validate_jar_argument_is_valid),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.IGNORE, ArgValueCase.IGNORE]
            ]
        }

    def define_rejected_missing_eventlogs(self):
        self.rejected['Missing Eventlogs'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd. Cannot define eventlogs from input '
                                '(platform, cluster, and eventlogs)'),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED]
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
    estimation_model: Optional[QualEstimationModel] = None
    cpu_cluster_price: Optional[float] = None
    estimated_gpu_cluster_price: Optional[float] = None
    cpu_discount: Optional[int] = None
    gpu_discount: Optional[int] = None
    global_discount: Optional[int] = None

    def init_tool_args(self):
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['savingsCalculations'] = True
        self.p_args['toolArgs']['targetPlatform'] = self.target_platform
        self.p_args['toolArgs']['cpuClusterPrice'] = self.cpu_cluster_price
        self.p_args['toolArgs']['estimatedGpuClusterPrice'] = self.estimated_gpu_cluster_price
        self.p_args['toolArgs']['cpuDiscount'] = self.cpu_discount
        self.p_args['toolArgs']['gpuDiscount'] = self.gpu_discount
        self.p_args['toolArgs']['globalDiscount'] = self.global_discount
        # check the filter_apps argument
        if self.filter_apps is None:
            self.p_args['toolArgs']['filterApps'] = QualFilterApp.get_default()
        else:
            self.p_args['toolArgs']['filterApps'] = self.filter_apps
        # check the reshapeType argument
        if self.gpu_cluster_recommendation is None:
            self.p_args['toolArgs']['gpuClusterRecommendation'] = QualGpuClusterReshapeType.get_default()
        else:
            self.p_args['toolArgs']['gpuClusterRecommendation'] = self.gpu_cluster_recommendation

        # check the estimationModel argument
        if self.estimation_model is None:
            self.p_args['toolArgs']['estimationModel'] = QualEstimationModel.get_default()
        else:
            self.p_args['toolArgs']['estimationModel'] = self.estimation_model

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

    def is_concurrent_submission(self):
        return self.p_args['toolArgs']['estimationModel'] != QualEstimationModel.SPEEDUPS

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
        if not self.p_args['toolArgs']['savingsCalculations']:
            # if savingsCalculations is disabled, we cannot use savings filter
            if self.p_args['toolArgs']['filterApps'] == QualFilterApp.SAVINGS:
                self.p_args['toolArgs']['filterApps'] = QualFilterApp.SPEEDUPS

        # process JVM arguments
        self.process_jvm_args()

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
                    'jvmMaxHeapSize': self.p_args['toolArgs']['jvmMaxHeapSize'],
                    'jvmGC': self.p_args['toolArgs']['jvmGC']
                },
                'jobResources': self.p_args['toolArgs']['jobResources']
            },
            'savingsCalculations': self.p_args['toolArgs']['savingsCalculations'],
            'eventlogs': self.eventlogs,
            'filterApps': QualFilterApp.fromstring(self.p_args['toolArgs']['filterApps']),
            'toolsJar': self.p_args['toolArgs']['toolsJar'],
            'gpuClusterRecommendation': self.p_args['toolArgs']['gpuClusterRecommendation'],
            'estimationModel': self.p_args['toolArgs']['estimationModel'],
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
    driverlog: Optional[str] = None

    def determine_cluster_arg_type(self) -> ArgValueCase:
        cluster_case = super().determine_cluster_arg_type()
        if cluster_case == ArgValueCase.VALUE_B:
            # determine is this an autotuner file or not
            auto_tuner_prop_obj = AutoTunerPropMgr.load_from_file(self.cluster, raise_on_error=False)
            if auto_tuner_prop_obj:
                cluster_case = ArgValueCase.VALUE_C
                self.p_args['toolArgs']['autotuner'] = self.cluster
        return cluster_case

    def init_driverlog_argument(self):
        if self.driverlog is None:
            self.p_args['toolArgs']['driverlog'] = None
        else:
            if not CspPath.is_file_path(self.driverlog, raise_on_error=False):
                raise PydanticCustomError(
                    'file_path',
                    'Driver log file path is not valid\n  Error:')

            # the file cannot be a http_url
            if is_http_file(self.cluster):
                # we do not accept http://urls
                raise PydanticCustomError(
                    'invalid_argument',
                    f'Driver log file path cannot be a web URL path: {self.driverlog}\n  Error:')
            self.p_args['toolArgs']['driverlog'] = self.driverlog

    def init_tool_args(self):
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['autotuner'] = None
        self.init_driverlog_argument()

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

    def define_rejected_missing_eventlogs(self):
        if self.p_args['toolArgs']['driverlog'] is None:
            super().define_rejected_missing_eventlogs()

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
        if self.p_args['toolArgs']['driverlog'] is None:
            requires_event_logs = True
            rapid_options = {}
        else:
            requires_event_logs = False
            rapid_options = {
                'driverlog': self.p_args['toolArgs']['driverlog']
            }

        # process JVM arguments
        self.process_jvm_args()

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
                    'jvmMaxHeapSize': self.p_args['toolArgs']['jvmMaxHeapSize'],
                    'jvmGC': self.p_args['toolArgs']['jvmGC']
                },
                'jobResources': self.p_args['toolArgs']['jobResources']
            },
            'eventlogs': self.eventlogs,
            'requiresEventlogs': requires_event_logs,
            'rapidOptions': rapid_options,
            'toolsJar': self.p_args['toolArgs']['toolsJar'],
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


@dataclass
@register_tool_arg_validator('prediction')
class PredictUserArgModel(AbsToolUserArgModel):
    """
    Represents the arguments collected by the user to run the prediction tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    qual_output: str = None
    prof_output: str = None

    def build_tools_args(self) -> dict:
        return {
            'runtimePlatform': self.platform,
            'qual_output': self.qual_output,
            'prof_output': self.prof_output,
            'output_folder': self.output_folder,
            'platformOpts': {}
        }
