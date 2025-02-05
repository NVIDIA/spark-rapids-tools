# Copyright (c) 2025, NVIDIA CORPORATION.
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
from logging import Logger
from typing import List, ClassVar

import pandas as pd

from spark_rapids_pytools.common.prop_manager import YAMLPropertiesContainer
from spark_rapids_pytools.common.utilities import Utils, ToolLogging
from spark_rapids_tools import CspPath
from spark_rapids_tools.configuration.submission.distributed_config import DistributedSubmissionConfig
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
    # Submission configuration provided by the user
    user_submission_configs: DistributedSubmissionConfig = field(default=None, init=True)
    # Path where the CLI stores the output from JAR tool
    cli_output_path: CspPath = field(default=None, init=True)
    # Command line arguments to run the JAR tool
    jar_cmd_args: JarCmdArgs = field(default=None, init=True)
    # Configuration properties defined by the tool
    props: YAMLPropertiesContainer = field(default=None, init=False)
    # Environment variables to be passed to the Spark job
    env_vars: dict = field(default_factory=dict, init=False)
    spark_session_builder: SparkSessionBuilder = field(default=None, init=False)
    logger: Logger = field(default=None, init=False)
    name: ClassVar[str] = 'distributed-tools'

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed')
        # Load the configuration properties defined by the tool
        config_path = Utils.resource_path(f'{self.name}-conf.yaml')
        self.props = YAMLPropertiesContainer(prop_arg=config_path)
        # Validate the environment
        self._validate_environment()
        # Create the cache directory if it does not exist
        local_cache_path = self._get_local_cache_path()
        if not local_cache_path.exists():
            local_cache_path.create_dirs(exist_ok=True)

    def _validate_environment(self):
        """
        Validate required environment variables and populate the env_vars dictionary.
        """
        required_vars = self.props.get_value('requiredEnvVariables')
        for var in required_vars:
            assert os.getenv(var) is not None, f'{var} environment variable is not set.'
            self.env_vars[var] = os.getenv(var)

        assert os.getenv('SPARK_HOME') in os.getenv('PYTHONPATH'), \
            'SPARK_HOME is not in PYTHONPATH. Set \'export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH\'.'

    def run_as_spark_app(self):
        """
        Entry method to run the tool as a Spark application.
        This method includes try-except block to catch any exceptions and log FAILED status
        in the output directory.
        """
        cli_jar_output_path = self._get_cli_jar_output_path()
        cli_jar_output_path.create_dirs()
        try:
            self._run_as_spark_app_internal()
        except Exception as e:  # pylint: disable=broad-except
            # TODO: Keeping traceback for debugging purposes. Remove it before production.
            traceback.print_exc()
            exception_msg = f'Failed to run the tool as a Spark application: {str(e)}'
            app_status = FailureAppStatus(eventlog_path=self.jar_cmd_args.event_logs_path, description=exception_msg)
            self._write_to_csv(app_status, cli_jar_output_path)

    def _run_as_spark_app_internal(self):
        """
        Internal method to run the tool as a Spark application.
        """
        # Get remote path for executor output and create necessary directories
        remote_executor_output_path = self._get_remote_executor_output_path()
        remote_executor_output_path.create_dirs()
        self.logger.info('Setting remote executor output path to: %s', remote_executor_output_path)

        # List all event log files from the provided event logs path
        event_log_path = CspPath(self.jar_cmd_args.event_logs_path)
        eventlog_files = CspFs.list_all_files(event_log_path)

        # Initialize the JAR runner. We convert submission_cmd to a dictionary to pass it to the runner.
        # Assumption: Path to SPARK_HOME, JAVA_HOME etc., on worker nodes are same as the driver node.
        jar_runner = SparkJarRunner(remote_executor_output_path=str(remote_executor_output_path),
                                    jar_cmd_args=self.jar_cmd_args,
                                    env_vars=self.env_vars)
        run_jar_command_fn = jar_runner.construct_jar_cmd_map_func()

        # Submit the Spark job and collect the application statuses
        self.spark_session_builder = self._create_spark_session_builder()
        job_submitter = SparkJobSubmitter(spark_session_builder=self.spark_session_builder,
                                          log_file_path=self._get_log_file_path())
        app_statuses = job_submitter.submit_map_job(map_func=run_jar_command_fn, input_list=eventlog_files)

        # Write any failed app statuses to HDFS
        self._write_failed_app_statuses_to_remote(app_statuses, remote_executor_output_path)

        # Combine output from the Spark job and qualification results
        output_combiner = QualificationOutputCombiner(cli_jar_output_path=self._get_cli_jar_output_path(),
                                                      remote_executor_output_path=remote_executor_output_path)
        output_combiner.combine()

        # Clean up any resources after processing
        self._cleanup()

    def _create_spark_session_builder(self) -> SparkSessionBuilder:
        """
        Submit the Spark job using the SparkJobManager
        """
        spark_properties = self.user_submission_configs.spark_properties if self.user_submission_configs else []
        return SparkSessionBuilder(
            spark_properties=spark_properties,
            props=self.props.get_value('sparkSessionBuilder'),
            local_cache_path=self._get_local_cache_path(),
            dependencies_paths=[self.jar_cmd_args.tools_jar_path, self.jar_cmd_args.jvm_log_file])

    def _write_failed_app_statuses_to_remote(self,
                                             app_statuses: List[AppStatusResult],
                                             remote_executor_output_path: CspPath):
        """
        Write the failed application statuses to each application's output directory in remote storage.
        E.g. hdfs:///path/qual_2024xxx/<eventlog_name>/rapids_4_spark_qualification_output/
        """
        jar_output_dir_name = self.props.get_value('tool', 'qualification', 'jarOutputDirName')
        for app_status in app_statuses:
            if app_status.status == AppStatus.FAILURE:
                file_name = os.path.basename(app_status.eventlog_path)
                remote_jar_output_path = (remote_executor_output_path
                                          .create_sub_path(file_name)
                                          .create_sub_path(jar_output_dir_name))
                self._write_to_csv(app_status, remote_jar_output_path)

    def _write_to_csv(self, app_status: AppStatusResult, jar_output_path: CspPath) -> None:
        """
        Helper function to write the application status to status CSV file.
        """
        status_csv_file_name = self.props.get_value('tool', 'qualification', 'statusCsvFileName')
        status_csv_file_path = jar_output_path.create_sub_path(status_csv_file_name)
        with status_csv_file_path.open_output_stream() as f:
            pd.DataFrame([app_status.to_dict()]).to_csv(f, index=False)

    def _cleanup(self):
        """
        Perform any necessary cleanup
        """
        self.spark_session_builder.cleanup()

    # Getter methods
    def _get_local_cache_path(self) -> LocalPath:
        """
        Get path to local cache directory.
        """
        return LocalPath(self.props.get_value('cacheDirPath'))

    def _get_remote_executor_output_path(self) -> CspPath:
        """
        Get path to remote executor output directory.
        Note: currently only HDFS is supported.
        """
        output_dir_name = self.cli_output_path.base_name()
        if not self.user_submission_configs or not self.user_submission_configs.remote_cache_dir:
            raise ValueError('Please provide the remote cache directory in the configuration file.')
        remote_cache_path = CspPath(self.user_submission_configs.remote_cache_dir)
        if remote_cache_path.protocol_prefix != HdfsPath.protocol_prefix:
            # TODO: Add support for other protocols
            remote_cache_path = HdfsPath(f'{HdfsPath.protocol_prefix}{remote_cache_path.no_scheme}')
        return remote_cache_path.create_sub_path(output_dir_name)

    def _get_cli_jar_output_path(self) -> CspPath:
        """
        Function to get the CLI's jar output path
        """
        jar_output_dir_name = self.props.get_value('tool', 'qualification', 'jarOutputDirName')
        return self.cli_output_path.create_sub_path(jar_output_dir_name)

    def _get_log_file_path(self) -> CspPath:
        """
        Function to get the log file path for the distributed tool.
        """
        log_file_name = self.props.get_value('outputFiles', 'logFileName')
        return self.cli_output_path.create_sub_path(log_file_name)
