# Copyright (c) 2025, NVIDIA CORPORATION.
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

""" Configuration file for distributed submission mode """
from typing import List, Optional

from pydantic import Field

from spark_rapids_tools.configuration.common import SparkProperty, SubmissionConfig
from spark_rapids_tools.configuration.tools_config import ToolsConfig


class DistributedSubmissionConfig(SubmissionConfig):
    """Configuration class for distributed submission mode"""
    remote_cache_dir: str = Field(
        description='Remote cache directory where the intermediate output data from each task will be stored. '
                    'Default is hdfs:///tmp/spark_rapids_distributed_tools_cache.',
        default=['hdfs:///tmp/spark_rapids_distributed_tools_cache']
    )

    spark_properties: List[SparkProperty] = Field(
        default_factory=list,
        description='List of Spark properties to be used for the Spark session.',
        examples=[{'name': 'spark.executor.memory', 'value': '4g'},
                  {'name': 'spark.executor.cores', 'value': '4'}]
    )


class DistributedToolsConfig(ToolsConfig):
    """Container for the distributed submission mode configurations. This is the parts of the configuration
    that can be passed as an input to the CLI"""
    submission: Optional[DistributedSubmissionConfig] = Field(
        default=None,
        description='Configuration related to distributed submission mode.')
