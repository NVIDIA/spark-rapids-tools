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
from typing import Optional, Any, ClassVar, Callable, Type, Dict, Union

from pydantic import model_validator, ValidationError
from pydantic.dataclasses import dataclass
from pydantic_core import PydanticCustomError

from spark_rapids_pytools.cloud_api.sp_types import DeployMode
from spark_rapids_pytools.common.utilities import ToolLogging, Utils
from spark_rapids_tools.cloud import ClientCluster
from spark_rapids_tools.utils import AbstractPropContainer, is_http_file
from ..configuration.tools_config import ToolsConfig
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


def register_tool_arg_validator(validator_key: str) -> Callable:
    def decorator(cls: type) -> type:
        cls.validator_name = validator_key
        user_arg_validation_registry[validator_key].name = validator_key
        user_arg_validation_registry[validator_key].validator_class = cls
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
    validator_name: ClassVar[str] = None

    @classmethod
    def create_tool_args(cls, validator_arg: Union[str, dict], *args: Any, cli_class: str = 'ToolsCLI',
                         cli_name: str = 'spark_rapids', **kwargs: Any) -> Optional[dict]:
        """
        A factory method to create the tool arguments based on the validator argument.
        :param validator_arg: Union type to accept either a dictionary or a string. This is required
                              because some validators such as the estimationModel can be used within
                              the context of different tools.
        :param args: to be passed to the actual validator class
        :param kwargs: to be passed to the actual validator class
        :return: the processed arguments or None if the validation fails.
        """
        if isinstance(validator_arg, dict):
            tool_name = validator_arg.get('toolName')
            validator_name = validator_arg.get('validatorName')
        else:
            tool_name = validator_arg
            validator_name = validator_arg
        cls.logger = ToolLogging.get_and_setup_logger('spark_rapids_tools.argparser')
        try:
            impl_entry = user_arg_validation_registry.get(validator_name)
            impl_class = impl_entry.validator_class
            new_obj = impl_class(*args, **kwargs)
            return new_obj.build_tools_args()
        except (ValidationError, PydanticCustomError) as e:
            impl_class.logger.error('Validation err: %s\n', e)
            dump_tool_usage(cli_class, cli_name, tool_name)
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
        self.logger.info('Detected platform from eventlogs prefix: %s', self.p_args['toolArgs']['platform'].name)

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

    def define_invalid_arg_cases(self) -> None:
        pass

    def define_detection_cases(self) -> None:
        pass

    def define_extra_arg_cases(self) -> None:
        pass

    def build_tools_args(self) -> dict:
        pass

    def apply_arg_cases(self, cases_list: list) -> None:
        for curr_cases in cases_list:
            for case_key, case_value in curr_cases.items():
                if any(ArgValueCase.array_equal(self.argv_cases, case_i) for case_i in case_value['cases']):
                    # debug the case key
                    self.logger.info('...applying argument case: %s', case_key)
                    case_value['callable']()

    def apply_all_arg_cases(self) -> None:
        self.apply_arg_cases([self.rejected, self.detected, self.extra])

    def validate_arguments(self) -> None:
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

    def post_platform_assignment_validation(self) -> None:
        # Update argv_cases to reflect the platform
        self.argv_cases[0] = ArgValueCase.VALUE_A
        # Any validation post platform assignment should be done here
        self.apply_arg_cases([self.rejected, self.extra])


@dataclass
@register_tool_arg_validator('estimation_model_args')
class EstimationModelArgProcessor(AbsToolUserArgModel):
    """
    Class to validate the arguments of the EstimationModel
    """
    estimation_model: Optional[QualEstimationModel] = None
    custom_model_file: Optional[str] = None

    def init_tool_args(self) -> None:
        if self.estimation_model is None:
            self.p_args['toolArgs']['estimationModel'] = QualEstimationModel.get_default()
        else:
            self.p_args['toolArgs']['estimationModel'] = self.estimation_model

    def init_arg_cases(self):
        # currently, the estimation model is set to XGBOOST by default.
        # so, we should not have undefined case
        self.argv_cases.append(ArgValueCase.VALUE_A)
        self.argv_cases.append(self.validate_custom_file_arg_is_valid())

    @model_validator(mode='after')
    def validate_arg_cases(self) -> 'EstimationModelArgProcessor':
        # shortcircuit to fail early
        self.validate_arguments()
        return self

    def validate_custom_model_on_valid_model(self) -> None:
        # custom model file is valid iff estimationModel is xgboost
        selected_model = self.p_args['toolArgs']['estimationModel']
        if selected_model != QualEstimationModel.XGBOOST:
            raise PydanticCustomError(
                'invalid_argument',
                'Cannot set custom estimation model when the estimation_model argument '
                f'is set to [{selected_model}]. Only valid for [{QualEstimationModel.XGBOOST}].')

    def define_invalid_arg_cases(self) -> None:
        super().define_invalid_arg_cases()
        self.rejected['Custom model file is valid only with XGBOOST estimation model'] = {
            'valid': False,
            'callable': partial(self.validate_custom_model_on_valid_model),
            'cases': [
                [ArgValueCase.VALUE_A, ArgValueCase.VALUE_B]
            ]
        }

    def validate_custom_file_arg_is_valid(self) -> ArgValueCase:
        # only json files are accepted
        self.p_args['toolArgs']['customModelFile'] = self.custom_model_file
        if self.custom_model_file is not None:
            if not CspPath.is_file_path(self.custom_model_file,
                                        extensions=['json'],
                                        raise_on_error=False):
                raise PydanticCustomError(
                    'custom_model_file',
                    f'model file path {self.custom_model_file} is not valid. '
                    'It is expected to be a valid JSON file.\n  Error:')
            return ArgValueCase.VALUE_B
        return ArgValueCase.UNDEFINED

    def build_tools_args(self) -> dict:
        xgboost_enabled = self.p_args['toolArgs']['estimationModel'] == QualEstimationModel.XGBOOST
        self.p_args['toolArgs']['xgboostEnabled'] = xgboost_enabled
        return self.p_args['toolArgs']


@dataclass
class ToolUserArgModel(AbsToolUserArgModel):
    """
    Abstract class that represents the arguments collected by the user to run the tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    eventlogs: Optional[str] = None
    jvm_heap_size: Optional[int] = None
    jvm_threads: Optional[int] = None
    tools_config_path: Optional[str] = None

    def is_concurrent_submission(self) -> bool:
        return False

    def process_jvm_args(self) -> None:
        # JDK8 uses parallel-GC by default. Set the GC algorithm to G1GC
        self.p_args['toolArgs']['jvmGC'] = '+UseG1GC'
        jvm_heap = self.jvm_heap_size
        if jvm_heap is None:
            # set default GC heap size based on the virtual memory of the host.
            jvm_heap = Utilities.calculate_jvm_max_heap_in_gb()
        # check if both tools are going to run concurrently, then we need to reduce the heap size
        # To reduce possibility of OOME, each core-tools thread should be running with at least 8 GB
        # of heap.
        adjusted_resources = Utilities.adjust_tools_resources(jvm_heap,
                                                              jvm_processes=2 if self.is_concurrent_submission() else 1,
                                                              jvm_threads=self.jvm_threads)
        self.p_args['toolArgs']['jvmMaxHeapSize'] = jvm_heap
        self.p_args['toolArgs']['jobResources'] = adjusted_resources
        self.p_args['toolArgs']['log4jPath'] = Utils.resource_path('dev/log4j.properties')

    def load_tools_config(self) -> None:
        """
        Load the tools config file if it is provided. It creates a ToolsConfig object and sets it
        in the toolArgs without processing the actual dependencies.
        :return: None
        """
        self.p_args['toolArgs']['toolsConfig'] = None
        if self.tools_config_path is not None:
            # the CLI provides a tools config file
            try:
                self.p_args['toolArgs']['toolsConfig'] = ToolsConfig.load_from_file(self.tools_config_path)
            except ValidationError as ve:
                # If required, we can dump the expected specification by appending
                # 'ToolsConfig.get_schema()' to the error message
                raise PydanticCustomError(
                    'invalid_config',
                    f'Tools config file path {self.tools_config_path} could not be loaded. '
                    'It is expected to be a valid configuration YAML file.'
                    f'\n  Error:{ve}\n') from ve

    def init_extra_arg_cases(self) -> list:
        if self.eventlogs is None:
            return [ArgValueCase.UNDEFINED]
        return [ArgValueCase.VALUE_A]

    def define_invalid_arg_cases(self) -> None:
        super().define_invalid_arg_cases()
        self.define_rejected_missing_eventlogs()
        self.rejected['Missing Platform argument'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd without platform argument. Re-run the command '
                                'providing the platform argument.'),
            'cases': [
                [ArgValueCase.UNDEFINED, ArgValueCase.IGNORE, ArgValueCase.IGNORE]
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
        self.rejected['Cluster By Properties Cannot go with OnPrem'] = {
            'valid': False,
            'callable': partial(self.validate_onprem_with_cluster_props_without_eventlogs),
            'cases': [
                [ArgValueCase.VALUE_A, ArgValueCase.VALUE_B, ArgValueCase.UNDEFINED]
            ]
        }
        self.rejected['Jar Argument'] = {
            'valid': False,
            'callable': partial(self.validate_jar_argument_is_valid),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.IGNORE, ArgValueCase.IGNORE]
            ]
        }

    def define_rejected_missing_eventlogs(self) -> None:
        self.rejected['Missing Eventlogs'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd. Cannot define eventlogs from input '
                                '(platform, cluster, and eventlogs)'),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.UNDEFINED, ArgValueCase.UNDEFINED]
            ]
        }

    def define_detection_cases(self) -> None:
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
    filter_apps: Optional[QualFilterApp] = None
    estimation_model_args: Optional[Dict] = dataclasses.field(default_factory=dict)

    def init_tool_args(self) -> None:
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['savingsCalculations'] = False
        # check the filter_apps argument
        if self.filter_apps is None:
            self.p_args['toolArgs']['filterApps'] = QualFilterApp.get_default()
        else:
            self.p_args['toolArgs']['filterApps'] = self.filter_apps
        # Check the estimationModel argument
        # This assumes that the EstimationModelArgProcessor was used to process the arguments before
        # constructing this validator.
        if self.estimation_model_args is None or not self.estimation_model_args:
            def_model = QualEstimationModel.get_default()
            self.p_args['toolArgs']['estimationModelArgs'] = QualEstimationModel.create_default_model_args(def_model)
        else:
            self.p_args['toolArgs']['estimationModelArgs'] = self.estimation_model_args

    @model_validator(mode='after')
    def validate_arg_cases(self) -> 'QualifyUserArgModel':
        # shortcircuit to fail early
        self.validate_arguments()
        return self

    def is_concurrent_submission(self) -> bool:
        return self.p_args['toolArgs']['estimationModelArgs']['xgboostEnabled']

    def build_tools_args(self) -> dict:
        # At this point, if the platform is still none, then we can set it to the default value
        # which is the onPrem platform.
        runtime_platform = self.get_or_set_platform()
        # process JVM arguments
        self.process_jvm_args()
        # process the tools config file
        self.load_tools_config()

        # finally generate the final values
        wrapped_args = {
            'runtimePlatform': runtime_platform,
            'outputFolder': self.output_folder,
            'toolsConfig': self.p_args['toolArgs']['toolsConfig'],
            'platformOpts': {
                'credentialFile': None,
                'deployMode': DeployMode.LOCAL
            },
            'migrationClustersProps': {
                'cpuCluster': self.cluster,
                'gpuCluster': None
            },
            'jobSubmissionProps': {
                'remoteFolder': None,
                'platformArgs': {
                    'jvmMaxHeapSize': self.p_args['toolArgs']['jvmMaxHeapSize'],
                    'jvmGC': self.p_args['toolArgs']['jvmGC'],
                    'Dlog4j.configuration': self.p_args['toolArgs']['log4jPath']
                },
                'jobResources': self.p_args['toolArgs']['jobResources']
            },
            'savingsCalculations': self.p_args['toolArgs']['savingsCalculations'],
            'eventlogs': self.eventlogs,
            'filterApps': QualFilterApp.fromstring(self.p_args['toolArgs']['filterApps']),
            'toolsJar': self.p_args['toolArgs']['toolsJar'],
            'estimationModelArgs': self.p_args['toolArgs']['estimationModelArgs']
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

    def init_driverlog_argument(self) -> None:
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

    def init_tool_args(self) -> None:
        self.p_args['toolArgs']['platform'] = self.platform
        self.p_args['toolArgs']['autotuner'] = None
        self.init_driverlog_argument()

    def define_invalid_arg_cases(self) -> None:
        super().define_invalid_arg_cases()
        self.rejected['Autotuner requires eventlogs'] = {
            'valid': False,
            'callable': partial(self.raise_validation_exception,
                                'Cannot run tool cmd. AutoTuner requires eventlogs argument'),
            'cases': [
                [ArgValueCase.IGNORE, ArgValueCase.VALUE_C, ArgValueCase.UNDEFINED]
            ]
        }

    def define_rejected_missing_eventlogs(self) -> None:
        if self.p_args['toolArgs']['driverlog'] is None:
            super().define_rejected_missing_eventlogs()

    def define_detection_cases(self) -> None:
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
        # process the tools config file
        self.load_tools_config()
        # finally generate the final values
        wrapped_args = {
            'runtimePlatform': runtime_platform,
            'toolsConfig': self.p_args['toolArgs']['toolsConfig'],
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
                    'jvmGC': self.p_args['toolArgs']['jvmGC'],
                    'Dlog4j.configuration': self.p_args['toolArgs']['log4jPath']
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
@register_tool_arg_validator('prediction')
class PredictUserArgModel(AbsToolUserArgModel):
    """
    Represents the arguments collected by the user to run the prediction tool.
    This is used as doing preliminary validation against some of the common pattern
    """
    qual_output: str = None
    prof_output: str = None
    estimation_model_args: Optional[Dict] = dataclasses.field(default_factory=dict)

    def build_tools_args(self) -> dict:
        if self.estimation_model_args is None or not self.estimation_model_args:
            def_model = QualEstimationModel.XGBOOST
            self.p_args['toolArgs']['estimationModelArgs'] = QualEstimationModel.create_default_model_args(def_model)
        else:
            self.p_args['toolArgs']['estimationModelArgs'] = self.estimation_model_args
        return {
            'runtimePlatform': self.platform,
            'qual_output': self.qual_output,
            'prof_output': self.prof_output,
            'output_folder': self.output_folder,
            'estimationModelArgs': self.p_args['toolArgs']['estimationModelArgs'],
            'platformOpts': {}
        }


@dataclass
@register_tool_arg_validator('train')
class TrainUserArgModel(AbsToolUserArgModel):
    """
    Represents the arguments collected by the user to run the training tool.
    """
    dataset: str = None
    model: Optional[str] = None
    n_trials: Optional[int] = None
    base_model: Optional[str] = None
    features_csv_dir: Optional[str] = None

    def build_tools_args(self) -> dict:
        runtime_platform = CspEnv.fromstring(self.platform)
        return {
            'runtimePlatform': runtime_platform,
            'dataset': self.dataset,
            'model': self.model,
            'output_folder': self.output_folder,
            'n_trials': self.n_trials,
            'base_model': self.base_model,
            'features_csv_dir': self.features_csv_dir,
            'platformOpts': {},
        }


@dataclass
@register_tool_arg_validator('stats')
class StatsUserArgModel(AbsToolUserArgModel):
    """
    Represents the arguments collected by the user to run the stats tool.
    """
    qual_output: str = None
    config_path: Optional[str] = None
    output_folder: Optional[str] = None

    def build_tools_args(self) -> dict:
        return {
            'runtimePlatform': self.platform,
            'config_path': self.config_path,
            'output_folder': self.output_folder,
            'qual_output': self.qual_output,
            'platformOpts': {}
        }


@dataclass
@register_tool_arg_validator('generate_instance_description')
class InstanceDescriptionUserArgModel(AbsToolUserArgModel):
    """
    Represents the arguments to run the generate_instance_description tool.
    """
    target_platform: str = None
    accepted_platforms = ['dataproc', 'emr', 'databricks-azure']

    def validate_platform(self) -> None:
        if self.target_platform not in self.accepted_platforms:
            raise PydanticCustomError('invalid_argument',
                                      f'Platform \'{self.target_platform}\' is not in ' +
                                      f'accepted platform list: {self.accepted_platforms}.')

    def build_tools_args(self) -> dict:
        self.validate_platform()
        return {
            'targetPlatform': CspEnv(self.target_platform),
            'output_folder': self.output_folder,
            'platformOpts': {},
        }
