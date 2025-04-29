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

"""Configuration for hash_util."""

from typing import List, Optional, Union
from pydantic import Field
from spark_rapids_tools.configuration.common import BaseConfig
from spark_rapids_tools.storagelib.csppath import CspPathT
from spark_rapids_tools.utils.propmanager import AbstractPropContainer


class HashConfig(BaseConfig):
    """Configuration for hash_util."""
    remove_nodes_exact: List[str] = Field(
        default=[],
        description='List of nodes to remove exactly.',
        examples=['Exchange', 'Sort']
    )
    remove_nodes_prefix: List[str] = Field(
        default=[],
        description='List of nodes to remove by prefix.',
        examples=['AdaptiveSparkPlan', 'ColumnarToRow', 'InputAdapter', 'ReusedExchange', 'WholeStageCodegen']
    )
    rename_nodes: dict[str, str] = Field(
        default={},
        description='Dictionary of nodes to rename by prefix.',
        examples={
            'BucketUnion': 'Union',
            'ObjectHashAggregate': 'HashAggregate',
            'ShuffledHashJoin': 'SortMergeJoin'
        }
    )

    @classmethod
    def load_from_file(cls, file_path: Union[str, CspPathT]) -> Optional['HashConfig']:
        """Load the Hash configuration from a file."""
        prop_container = AbstractPropContainer.load_from_file(file_path)
        return cls(**prop_container.props)

    @classmethod
    def get_schema(cls) -> str:
        """Returns a JSON schema of the Qualx configuration."""
        return cls.model_json_schema()
