# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Main module for distributed execution of Spark Rapids Tools JAR on Spark.
"""

import os
import traceback
from dataclasses import dataclass, field
from typing import List, ClassVar

import pandas as pd

from spark_rapids_pytools.common.prop_manager import YAMLPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils, ToolLogging
from spark_rapids_tools import CspPath
from spark_rapids_tools.configuration.distributed_tools_config import DistributedToolsConfig
from spark_rapids_tools.storagelib import HdfsPath, LocalPath, CspFs
from spark_rapids_tools_distributed.jar_cmd_args import JarCmdArgs
from spark_rapids_tools_distributed.output_processing.combiner import QualificationOutputCombiner
from spark_rapids_tools_distributed.spark_management.spark_job_submitter import SparkJobSubmitter
from spark_rapids_tools_distributed.spark_management.spark_session_builder import SparkSessionBuilder
from spark_rapids_tools_distributed.spark_map_task.jar_runner import SparkJarRunner
from spark_rapids_tools_distributed.spark_map_task.status_reporter import AppStatusResult, FailureAppStatus, AppStatus


@dataclass
class DistributedToolsExecutor:
    """
    Class to orchestrate the execution of the Spark Rapids Tools JAR on Spark.
    """
    distributed_tools_configs: DistributedToolsConfig = field(default=None, init=True)
    platform: str = field(default=None, init=True)
    output_folder: str = field(default=None, init=True)
    jar_cmd_args: JarCmdArgs = field(default=None, init=True)
    props: YAMLPropertiesContainer = field(default=None, init=False)
    spark_session_builder: SparkSessionBuilder = field(default=None, init=False)
    logger: ToolLogging = field(default=None, init=False)
    env_vars: dict = field(default_factory=dict, init=False)
    name: ClassVar[str] = 'distributed-tools'

    def __post_init__(self):
        self._validate_environment()
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed')

        # Load the configuration properties
        config_path = Utils.resource_path(f'{self.name}-conf.yaml')
        self.props = YAMLPropertiesContainer(prop_arg=config_path)

        # Create the cache directory if it does not exist
        cache_dir = self._get_local_cache_dir()
        if not FSUtil.resource_exists(cache_dir):
            FSUtil.make_dirs(cache_dir)

    def _validate_environment(self):
        """Validate required environment variables."""
        required_vars = ['SPARK_HOME', 'HADOOP_HOME', 'JAVA_HOME', 'PYTHONPATH']
        for var in required_vars:
            assert os.getenv(var) is not None, f'{var} environment variable is not set.'
            self.env_vars[var] = os.getenv(var)

        assert os.getenv('SPARK_HOME') in os.getenv('PYTHONPATH'), ('SPARK_HOME is not in PYTHONPATH. '
                                                                    'Include \'$SPARK_HOME/python\' in PYTHONPATH.')

    def run_as_spark_app(self):
        """
        Public method to run the tool as a Spark application.
        """
        jar_output_path = self._get_jar_output_path()
        jar_output_path.create_dirs()
        try:
            self._run_as_spark_app_internal()
        except Exception as e:  # pylint: disable=broad-except
            # TODO: Keeping traceback for debugging purposes. Remove it before production.
            traceback.print_exc()
            exception_msg = f'Failed to run the tool as a Spark application: {str(e)}'
            app_status = FailureAppStatus(eventlog_path=self.jar_cmd_args.event_logs_path, description=exception_msg)
            self._write_to_csv(app_status, jar_output_path)

    def _run_as_spark_app_internal(self):
        # Get HDFS path for executor output and create necessary directories
        executor_output_path = self._get_hdfs_executor_output_path()
        executor_output_path.create_dirs()

        # List all event log files from the provided event logs path
        event_log_path = CspPath(self.jar_cmd_args.event_logs_path)
        eventlog_files = CspFs.list_all_files(event_log_path)

        # Initialize the JAR runner. We convert submission_cmd to a dictionary to pass it to the runner.
        # Assumption: Path to SPARK_HOME, JAVA_HOME etc, on worker nodes are same as the driver node.
        jar_runner = SparkJarRunner(platform=self.platform,
                                    output_dir=str(executor_output_path),
                                    jar_cmd_args=self.jar_cmd_args,
                                    env_vars=self.env_vars)
        run_jar_command_fn = jar_runner.construct_jar_cmd_map_func()

        # Submit the Spark job and collect the application statuses
        self.spark_session_builder = self._create_spark_session_builder()
        job_submitter = SparkJobSubmitter(spark_session_builder=self.spark_session_builder,
                                          log_file_path=self._get_log_file_path())
        app_statuses = job_submitter.submit_map_job(map_func=run_jar_command_fn, input_list=eventlog_files)

        # Write any failed app statuses to HDFS
        self._write_failed_app_statuses_to_hdfs(app_statuses, executor_output_path)

        # Combine output from the Spark job and qualification results
        output_combiner = QualificationOutputCombiner(jar_output_folder=self._get_jar_output_path(),
                                                      executor_output_dir=executor_output_path)
        output_combiner.combine()

        # Clean up any resources after processing
        self._cleanup()

    def _create_spark_session_builder(self) -> SparkSessionBuilder:
        """Submit the Spark job using the SparkJobManager."""
        spark_properties = self.distributed_tools_configs.spark_properties if self.distributed_tools_configs else []
        return SparkSessionBuilder(
            spark_properties=spark_properties,
            props=self.props.get_value('sparkSessionBuilder'),
            cache_dir=self._get_local_cache_dir(),
            dependencies_paths=[self.jar_cmd_args.tools_jar_path, self.jar_cmd_args.jvm_log_file])

    def _write_failed_app_statuses_to_hdfs(self, app_statuses: List[AppStatusResult], executor_output_path: CspPath):
        """
        Write the failed application statuses to each application's output directory in HDFS.
        E.g. hdfs:///path/qual_2024xxx/<eventlog_name>/rapids_4_spark_qualification_output/
        """
        jar_output_dir_name = self.props.get_value('tool', 'qualification', 'jarOutputDirName')
        for app_status in app_statuses:
            if app_status.status == AppStatus.FAILURE:
                file_name = os.path.basename(app_status.eventlog_path)
                jar_output_path = (executor_output_path
                                   .create_sub_path(file_name)
                                   .create_sub_path(jar_output_dir_name))
                self._write_to_csv(app_status, jar_output_path)

    def _write_to_csv(self, app_status: AppStatusResult, jar_output_path: CspPath) -> None:
        status_csv_file_name = self.props.get_value('tool', 'qualification', 'statusCsvFileName')
        status_csv_file_path = jar_output_path.create_sub_path(status_csv_file_name)
        with status_csv_file_path.open_output_stream() as f:
            pd.DataFrame([app_status.to_dict()]).to_csv(f, index=False)

    def _cleanup(self):
        """Perform any necessary cleanup."""
        self.spark_session_builder.cleanup()

    # Getter methods
    def _get_local_cache_dir(self) -> str:
        return self.props.get_value('outputFiles', 'cacheDirPath')

    def _get_hdfs_executor_output_path(self) -> HdfsPath:
        output_folder_name = os.path.basename(self.output_folder)
        hdfs_cache_dir = HdfsPath(f'{HdfsPath.protocol_prefix}{self._get_local_cache_dir()}')
        return hdfs_cache_dir.create_sub_path(output_folder_name)

    def _get_jar_output_path(self) -> LocalPath:
        jar_output_dir_name = self.props.get_value('tool', 'qualification', 'jarOutputDirName')
        jar_output_path = os.path.join(self.output_folder, jar_output_dir_name)
        return LocalPath(jar_output_path)

    def _get_log_file_path(self) -> str:
        log_file_name = self.props.get_value('outputFiles', 'logFileName')
        return os.path.join(self.output_folder, log_file_name)
