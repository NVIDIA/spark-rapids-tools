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

""" Builds and configures the Spark session. """

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Any
from logging import Logger

from pyspark import SparkContext
from pyspark.sql import SparkSession

from spark_rapids_tools.configuration.common import SparkProperty
from spark_rapids_tools.storagelib import LocalPath
from spark_rapids_tools.utils.util import Utilities
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class SparkSessionBuilder:
    """
    Class responsible for building and configuring the Spark session
    """

    spark_properties: List[SparkProperty] = field(default=None, init=True)
    props: JSONPropertiesContainer = field(default=None, init=True)
    local_cache_path: LocalPath = field(default=None, init=True)
    dependencies_paths: List[str] = field(default=None, init=True)
    logger: Logger = field(default=None, init=False)
    spark: SparkSession = field(default=None, init=False)

    def __post_init__(self):
        logging.getLogger('py4j').setLevel(logging.ERROR)
        if isinstance(self.props, dict):
            self.props = JSONPropertiesContainer(prop_arg=self.props, file_load=False)
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed.spark_session_builder')
        self._initialize_spark_context()

    def _generate_spark_conf(self) -> Dict[str, Any]:
        """
        Generate Spark configurations by merging default and user-provided configurations.
        """
        # Retrieve default Spark configurations from properties
        default_spark_confs = self.props.get_value('sparkConfigs', 'default') or []
        default_confs = {conf['name']: conf['value'] for conf in default_spark_confs}
        # Retrieve user-provided Spark configurations, filtering protected ones
        user_confs = {prop.name: prop.value for prop in self.spark_properties}
        filtered_user_confs = self._filter_protected_spark_conf(user_confs)
        return {**default_confs, **filtered_user_confs}

    def _filter_protected_spark_conf(self, spark_conf: Dict[str, str]) -> Dict[str, str]:
        """
        Remove protected Spark configurations
        """
        return {k: v for k, v in spark_conf.items() if k not in self.props.get_value('sparkConfigs', 'protected')}

    def _initialize_spark_context(self):
        """
        Initialize the Spark session context with configurations
        """
        spark_builder = SparkSession.builder.appName(self.props.get_value('distributedToolsAppName'))
        spark_confs = self._generate_spark_conf()
        self.logger.info('Setting Spark configurations: \n%s', json.dumps(spark_confs, indent=4))

        for key, value in spark_confs.items():
            spark_builder.config(key, value)

        spark_builder.config('spark.submit.deployMode', 'client')
        spark_builder.config('spark.executorEnv.PYTHONPATH', os.environ['PYTHONPATH'])
        self.spark = spark_builder.getOrCreate()
        self._add_dependencies_to_spark_context()

    def _get_python_dependencies_path(self) -> List[LocalPath]:
        """
        Returns the list of Python dependencies to be added to the Spark context.

        Currently, the only dependency is the spark_rapids_tools_distributed module.
        """
        # Get the absolute path of the current script
        current_script_path = os.path.abspath(__file__)
        # Determine the module's directory path
        module_path = Path(current_script_path).parent.parent
        module_name = module_path.name
        # Define the base name for the zip file (without the archive extension)
        base_name = self.local_cache_path.create_sub_path(module_name)
        # Create a zip of the module directory
        actual_zip_path = LocalPath(
            Utilities.archive_directory(module_path.as_posix(), base_name.no_scheme)
        )
        if not actual_zip_path.exists():
            raise FileNotFoundError(f'Error while zipping Python dependencies. Zip file not found: {actual_zip_path}')
        return [actual_zip_path]

    def _add_dependencies_to_spark_context(self):
        """
        Adds dependencies and files to the Spark context
        """
        for dep_path in self.dependencies_paths:
            self.logger.info('Adding dependencies to Spark context: %s', dep_path)
            self.spark_context.addFile(dep_path)
        for local_dep_path in self._get_python_dependencies_path():
            self.logger.info('Adding Python dependencies to Spark context: %s', local_dep_path)
            self.spark_context.addPyFile(local_dep_path.no_scheme)

    @property
    def spark_context(self) -> SparkContext:
        """
        Return the Spark context
        """
        return self.spark.sparkContext

    def cleanup(self):
        """
        Cleanup the Spark session
        """
        self.spark_context.stop()
        self.logger.info('Spark session stopped.')
