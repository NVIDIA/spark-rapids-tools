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

""" SparkJobManager class to manage Spark jobs """

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import Logger
from typing import Callable, Iterator, List, Tuple

from pyspark.rdd import RDD

from spark_rapids_tools import CspPath
from spark_rapids_tools_distributed.spark_map_task.status_reporter import AppStatusResult
from spark_rapids_tools_distributed.spark_management.spark_session_builder import SparkSessionBuilder
from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class SparkJobSubmitter:
    """
    Class to handle the submission and management of Spark jobs
    """

    spark_session_builder: SparkSessionBuilder = field(default=None, init=True)
    log_file_path: CspPath = field(default=None, init=True)
    logger: Logger = field(default=None, init=False)

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed.spark_job_submitter')

    def _convert_input_to_rdd(self, input_list: Iterator[str]) -> RDD:
        """
        Convert the input list to an RDD
        """
        input_list_str = [str(path) for path in input_list]
        num_partitions = len(input_list_str)
        return self.spark_session_builder.spark_context.parallelize(input_list_str, numSlices=num_partitions)

    @staticmethod
    def _run_map_job(map_func: Callable, input_list_rdd: RDD) -> Tuple[list, timedelta]:
        """
        Run a map job on the given RDD
        """
        start_time = datetime.now()
        map_fn_result = input_list_rdd.map(map_func).collect()
        total_time = datetime.now() - start_time
        return map_fn_result, total_time

    def _write_output(self, logs_arr_list: List[List[str]], total_time: timedelta):
        """
        Write the job output to the log file
        """
        self.logger.info('Saving distributed job output to %s', self.log_file_path)
        logs_list = ['\n'.join(logs) for logs in logs_arr_list]
        output_str = '\n\n'.join(logs_list) + f'\nTotal Job Time: {total_time}'

        try:
            with self.log_file_path.open_output_stream() as f:
                f.write(output_str.encode('utf-8'))
        except IOError as e:
            self.logger.error('Failed to write to log file %s. Error: %s', self.log_file_path, e)
            raise

    def submit_map_job(self, map_func: Callable, input_list: Iterator[str]) -> List[AppStatusResult]:
        """
        Submit a map job and handle the results
        """
        input_list_rdd = self._convert_input_to_rdd(input_list)
        try:
            map_fn_result, total_time = self._run_map_job(map_func, input_list_rdd)
            logs_arr_list, app_statuses = zip(*map_fn_result)
            self._write_output(logs_arr_list, total_time)
            return list(app_statuses)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error during map job submission: %s', e)
            raise
