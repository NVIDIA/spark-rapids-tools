# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

"""The runtime configurations of the tools as defined by the user."""

from typing import List, Optional

from pydantic import Field

from spark_rapids_tools.configuration.common import RuntimeDependency, BaseConfig


class ToolsRuntimeConfig(BaseConfig):
    """The runtime configurations of the tools as defined by the user."""
    dependencies: List[RuntimeDependency] = Field(
        description='The list of runtime dependencies required by the tools java cmd. '
                    'Set this list to specify Spark binaries along with any other required jar '
                    'files (i.e., hadoop jars, gcp connectors,..etc.). '
                    'When specified, the default predefined dependencies will be ignored.')

    jvm_heap_size: Optional[int] = Field(
        default=None,
        description='The maximum heap size of the JVM in gigabytes. '
                    'If not set, a default value is calculated based on host memory.',
        examples=[16, 32])

    jvm_threads: Optional[int] = Field(
        default=None,
        description='Number of threads to use for parallel processing on the eventlogs batch. '
                    'If not set, a default value is calculated based on host cores and heap size.',
        examples=[4, 8])
