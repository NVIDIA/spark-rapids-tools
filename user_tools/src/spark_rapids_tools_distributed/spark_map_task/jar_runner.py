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
Module containing the SparkJarRunner class to run a JAR file in a Spark job.
This class defines the map function to be used in the Spark job to run JAR files.
"""
import hashlib
import os
import re
import socket
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, List, Tuple, ClassVar

from pyspark import SparkFiles

from spark_rapids_tools_distributed.jar_cmd_args import JarCmdArgs
from spark_rapids_tools_distributed.spark_map_task.status_reporter import AppStatus, AppStatusResult, SuccessAppStatus, FailureAppStatus


@dataclass
class SparkJarRunner:
    """
    Class to run a JAR file in a Spark job.
    """
    remote_executor_output_path: str = field(init=True)
    jar_cmd_args: JarCmdArgs = field(init=True)
    env_vars: dict = field(init=True)  # Environment variables to be used in the Spark job
    # Maximum length of the file name to be used as a directory name
    # Reference: https://en.wikipedia.org/wiki/Comparison_of_file_systems#Limits
    max_file_name_bytes: ClassVar[int] = 200
    default_file_name_sep: ClassVar[str] = '_'

    def get_env_var(self, key: str) -> str:
        """
        Get the environment variable value from the worker environment or the environment configuration
        provided by the driver.
        """
        env_var = os.getenv(key) or self.env_vars.get(key)
        if env_var is None:
            raise ValueError(f'Environment variable {key} not found in the environment or in the configuration.')
        return env_var

    def construct_jar_cmd_map_func(self) -> Callable[[str], Tuple[List[str], AppStatusResult]]:
        """
        Construct a function to be used as a map function for running JAR files.
        """
        def run_jar_map_func(file_path: str):
            # Sanitize the file name and create the output directory for the map task
            sanitized_file_name = self.sanitize_for_posix(os.path.basename(file_path))
            executor_output_path = os.path.join(self.remote_executor_output_path, sanitized_file_name)
            # Construct the JAR command and submit it as a map task
            jar_command = self._construct_jar_cmd(file_path, executor_output_path)
            # Execute the JAR command and capture the status, output, and execution time
            app_status, processing_time = self._submit_jar_cmd(jar_command)
            logs = self._generate_log_lines(file_path, executor_output_path, jar_command, processing_time, app_status)
            return logs, app_status
        return run_jar_map_func

    def _construct_jar_cmd(self, file_path: str, executor_output_path: str) -> List[str]:
        """
        Reconstructs the JAR command to be executed by the workers.
        """
        # Build the classpath using the tools JAR, Hadoop classpath, and Spark JARs
        classpath = ':'.join([
            SparkFiles.get(os.path.basename(self.jar_cmd_args.tools_jar_path)),
            self.jar_cmd_args.hadoop_classpath,
            f'{self.get_env_var("SPARK_HOME")}/jars/*'
        ])

        # Set the JVM log file path
        self._set_jvm_log_file_path()

        # Construct the JAR command
        return [
            # pylint: disable=line-too-long
            f'{self.get_env_var("JAVA_HOME")}/bin/java',       # Java executable path
            *self.jar_cmd_args.jvm_args,                       # JVM Arguments: Log configuration, memory settings, etc.
            '-cp', classpath,                                  # Classpath for dependencies
            self.jar_cmd_args.jar_main_class,                  # Spark RAPIDS Tools main class
            '--output-directory', executor_output_path,        # Tools Argument: Specify the output directory
            *self.jar_cmd_args.rapids_args,                    # Tools Argument: Other arguments for the Tools JAR
            file_path                                          # Tools Argument: Event logs path
        ]

    def _set_jvm_log_file_path(self) -> None:
        """
        Set the file path for the JVM log configuration from SparkFiles.
        """
        log_file_path = SparkFiles.get(os.path.basename(self.jar_cmd_args.jvm_log_file))

        for i, arg in enumerate(self.jar_cmd_args.jvm_args):
            if '-Dlog4j.configuration' in arg:
                self.jar_cmd_args.jvm_args[i] = f'-Dlog4j.configuration=file:{log_file_path}'
                break

    @staticmethod
    def _submit_jar_cmd(jar_command: List[str]) -> Tuple[AppStatusResult, timedelta]:
        """
        Executes a JAR command and captures its status, output, and execution time.

        :param jar_command: The JAR command to execute.
        :return: A tuple containing the logs generated during the execution and the status of the application.
        """
        start_time = datetime.now()
        try:
            result = subprocess.run(jar_command, check=True, capture_output=True, text=True)
            if result.returncode == 0:
                app_status = SuccessAppStatus(eventlog_path=jar_command[-1])
            else:
                app_status = FailureAppStatus(eventlog_path=jar_command[-1], description=result.stderr)
        except subprocess.CalledProcessError as ex:
            app_status = FailureAppStatus(eventlog_path=jar_command[-1], description=ex.stderr)
        except Exception as ex:  # pylint: disable=broad-except
            app_status = FailureAppStatus(eventlog_path=jar_command[-1], description=str(ex))
        finally:
            processing_time = datetime.now() - start_time
        return app_status, processing_time

    @classmethod
    def sanitize_for_posix(cls, filename: str) -> str:
        """
        Sanitize a filename to make it safe for use as a directory name on POSIX systems.

        This function removes or replaces characters that are not allowed or could cause issues in a directory name.
        It converts spaces to underscores, removes invalid characters, and enforces a maximum length of 255 characters.

        Examples:
            >>> SparkJarRunner.sanitize_for_posix('My File Name.txt')
            'My_File_Name.txt'

            >>> SparkJarRunner.sanitize_for_posix('example/file/name')
            'example_file_name'

            >>> SparkJarRunner.sanitize_for_posix('file*name|with<>invalid:chars')
            'filenamewithinvalidchars'

            >>> SparkJarRunner.sanitize_for_posix('!@#$%^&*()')
            'default_directory_name'

        :param filename: The filename to sanitize.
        :return: A sanitized version of the filename safe for use as a directory name.
        """
        sanitized = filename.strip()  # Remove leading/trailing whitespace
        sanitized = sanitized.replace(' ', cls.default_file_name_sep)  # Replace spaces with underscores
        sanitized = sanitized.replace('.', cls.default_file_name_sep)  # Replace dots with underscores
        sanitized = re.sub(r'[\/]', cls.default_file_name_sep, sanitized)  # Replace slashes with underscores
        sanitized = re.sub(r'[^\w\-]', cls.default_file_name_sep, sanitized)  # Replace any non-word or dash characters with underscores

        if len(sanitized) > cls.max_file_name_bytes:
            # If the sanitized file name is too long, hash it to ensure it fits within the FileSystem limits
            file_name_hash = str(hashlib.md5(sanitized.encode()).hexdigest())
            # Calculate how much space is available for truncating the file name
            remaining_space = cls.max_file_name_bytes - (len(file_name_hash) + 1)
            if remaining_space > 0:
                # Truncate the file name and append the hash
                sanitized = f'{sanitized[:remaining_space]}_{file_name_hash}'
            else:
                sanitized = file_name_hash

        return sanitized

    @staticmethod
    def _generate_log_lines(file_path: str, executor_output_path: str, jar_command: List[str],
                            processing_time: timedelta, app_status: AppStatusResult) -> List[str]:
        """
        Generate the log lines to be written to the output file.
        """
        return [
            'Processing Details:',
            '-------------------',
            f'Host: {socket.gethostname()}',
            f'Event Log Path: {file_path}',
            f'Executor Output Path: {executor_output_path}',
            f'Command: {" ".join(jar_command)}',
            f'Processing Time: {processing_time}',
            f'Status: {app_status.status.value}',
            f'Error Description:\n {app_status.description}' if app_status.status == AppStatus.FAILURE else '',
        ]
