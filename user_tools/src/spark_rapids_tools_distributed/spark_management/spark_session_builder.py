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

""" Builds and configures the Spark session. """

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from logging import Logger

from pyspark import SparkContext
from pyspark.sql import SparkSession

from spark_rapids_tools.configuration.distributed_tools_config import SparkProperty
from spark_rapids_tools.utils.util import Utilities
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class SparkSessionBuilder:
    """ Class responsible for building and configuring the Spark session """

    spark_properties: List[SparkProperty] = field(default=None, init=True)
    props: JSONPropertiesContainer = field(default=None, init=True)
    cache_dir: str = field(default=None, init=True)
    dependencies_paths: List[str] = field(default=None, init=True)
    logger: Logger = field(default=None, init=False)
    spark: SparkSession = field(default=None, init=False)

    def __post_init__(self):
        logging.getLogger('py4j').setLevel(logging.ERROR)
        if isinstance(self.props, dict):
            self.props = JSONPropertiesContainer(prop_arg=self.props, file_load=False)
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed.spark_session_builder')
        self._initialize_spark_context()

    def _parse_spark_conf(self) -> Dict[str, str]:
        """Parse the Spark configurations"""
        return {prop.name: prop.value for prop in self.spark_properties}

    def _generate_spark_conf(self, min_heap_memory_per_task=4, task_cpus=1) -> Dict[str, str]:
        """ Generate Spark configurations """
        user_spark_confs = self._parse_spark_conf()
        executor_memory_str = user_spark_confs.get('spark.executor.memory')
        # Default memory configuration if not provided by user
        if not executor_memory_str:
            default_memory = self.props.get_value('defaultExecutorMemory')
            spark = SparkSession.builder.appName('Generate Spark Config').getOrCreate()
            executor_memory_str = spark.conf.get('spark.executor.memory', default_memory)
            spark.stop()

        executor_memory_gb = Utilities.parse_memory_size_in_gb(executor_memory_str)
        max_tasks_per_executor = int(executor_memory_gb // min_heap_memory_per_task)
        spark_conf = {
            'spark.executor.instances': 1,
            'spark.executor.cores': max_tasks_per_executor,
            'spark.executor.memory': f'{int(executor_memory_gb)}g',
            'spark.task.cpus': task_cpus
        }

        # Merge user provided configurations after removing protected configurations
        spark_conf.update(self._filter_protected_spark_conf(user_spark_confs))
        return spark_conf

    def _filter_protected_spark_conf(self, spark_conf: Dict[str, str]) -> Dict[str, str]:
        """ Remove protected Spark configurations """
        return {k: v for k, v in spark_conf.items() if k not in self.props.get_value('protectedSparkConfigs')}

    def _initialize_spark_context(self):
        """ Initialize the Spark session context with configurations """
        spark_builder = SparkSession.builder.appName(self.props.get_value('distributedToolsAppName'))
        spark_confs = self._generate_spark_conf()
        self.logger.info('Setting Spark configurations: %s', json.dumps(spark_confs, indent=4))

        for key, value in spark_confs.items():
            spark_builder.config(key, value)

        spark_builder.config('spark.submit.deployMode', 'client')
        spark_builder.config('spark.executorEnv.PYTHONPATH', os.environ['PYTHONPATH'])
        self.spark = spark_builder.getOrCreate()
        self._add_files_to_spark_context()

    def _get_python_dependencies(self) -> List[str]:
        """Returns the list of Python dependencies to be added to the Spark context."""
        current_file_path = os.path.abspath(__file__)
        folder_path = Path(current_file_path).parent.parent
        module_name = folder_path.name
        dest_zip_path = os.path.join(self.cache_dir, module_name + '.zip')
        zip_path = Utilities.zip_folder(folder_path.as_posix(), dest_zip_path)
        return [zip_path]

    def _add_files_to_spark_context(self):
        """Adds dependencies and files to the Spark context."""
        for dep_path in self.dependencies_paths:
            self.logger.info('Adding dependencies to Spark context: %s', dep_path)
            self.spark_context.addFile(dep_path)
        for dep_path in self._get_python_dependencies():
            self.logger.info('Adding Python dependencies to Spark context: %s', dep_path)
            self.spark_context.addPyFile(dep_path)

    @property
    def spark_context(self) -> SparkContext:
        """ Return the Spark context """
        return self.spark.sparkContext

    def cleanup(self):
        """ Cleanup the Spark session """
        self.spark_context.stop()
        self.logger.info('Spark session stopped.')
