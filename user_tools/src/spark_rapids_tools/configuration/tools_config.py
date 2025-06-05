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

"""Container for the custom tools configurations. This is the parts of the configuration that can
be passed as an input to the CLI"""

import json
from typing import Union, Optional

from pydantic import Field, ValidationError

from spark_rapids_tools import CspPathT
from spark_rapids_tools.configuration.common import BaseConfig, SubmissionConfig
from spark_rapids_tools.configuration.runtime_conf import ToolsRuntimeConfig
from spark_rapids_tools.utils import AbstractPropContainer


class ToolsConfig(BaseConfig):
    """Main container for the user's defined tools configuration"""
    api_version: float = Field(
        description='The version of the API that the tools are using. '
                    'This is used to test the compatibility of the '
                    'configuration file against the current tools release.',
        examples=['1.0, 1.1'],
        le=1.2,  # minimum version compatible with the current tools implementation
        ge=1.0)

    runtime: Optional[ToolsRuntimeConfig] = Field(
        default=None,
        description='Configuration related to the runtime environment of the tools.')

    submission: Optional[SubmissionConfig] = Field(
        default=None,
        description='Configuration related to the submission.')

    @classmethod
    def load_from_file(cls, file_path: Union[str, CspPathT]) -> Optional['ToolsConfig']:
        """Load the tools configuration from a file"""
        try:
            prop_container = AbstractPropContainer.load_from_file(file_path)
            return cls(**prop_container.props)
        except ValidationError as e:
            # Do nothing. This is kept as a placeholder if we want to log the error inside the
            # class first
            raise e

    @classmethod
    def get_schema(cls) -> str:
        """Returns a JSON schema of the tools' configuration. This is useful for generating an API
        documentation of the model."""
        return json.dumps(cls.model_json_schema(), indent=2)
