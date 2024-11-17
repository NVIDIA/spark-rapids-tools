# Copyright (c) 2024, NVIDIA CORPORATION.
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

""" Configuration file for distributed tools """
from typing import List

from pydantic import BaseModel, Field


class SparkProperty(BaseModel):
    """Represents a single Spark property with a name and value."""
    name: str = Field(
        description='The name of the Spark property, e.g., "spark.executor.memory".')
    value: str = Field(
        description='The value of the Spark property, e.g., "4g".')


class DistributedToolsConfig(BaseModel):
    """Configuration class for distributed tools"""
    spark_properties: List[SparkProperty] = Field(
        default_factory=list,
        description='List of Spark properties to be used for the Spark session.',
        examples=[{'name': 'spark.executor.memory', 'value': '4g'},
                  {'name': 'spark.executor.cores', 'value': '4'}]
    )
