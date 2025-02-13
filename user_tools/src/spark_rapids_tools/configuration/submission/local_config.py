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

""" Configuration file for local submission mode """
from typing import Optional

from pydantic import Field

from spark_rapids_tools.configuration.common import SubmissionConfig
from spark_rapids_tools.configuration.tools_config import ToolsConfig


class LocalSubmissionConfig(SubmissionConfig):
    """Configuration class for local submission mode"""


class LocalToolsConfig(ToolsConfig):
    """Container for the local submission mode configurations. This is the parts of the configuration that
    can be passed as an input to the CLI"""
    submission: Optional[LocalSubmissionConfig] = Field(
        default=None,
        description='Configuration related to local submission mode.')
