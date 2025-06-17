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

"""Qualification Output Related Table Definitions"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class QualCoreColumnDef:
    """Represents a column definition in a qualification table."""
    name: str
    data_type: str
    description: str

    def __str__(self) -> str:
        return f'QualCoreColumnDef(name={self.name}, data_type={self.data_type})'


@dataclass
class QualCoreTableDef:
    """Represents a table definition for qualification reporting."""
    label: str
    description: str
    file_name: str
    scope: str
    columns: List[QualCoreColumnDef]
    file_format: Optional[str] = None  # Optional field, defaults to CSV if not specified

    def __str__(self) -> str:
        return (
            f'QualCoreTableDef(label={self.label}, '
            f'file_name={self.file_name}, '
            f'scope={self.scope}, '
            f'columns={len(self.columns)})'
        )

    def get_column_by_name(self, name: str) -> Optional[QualCoreColumnDef]:
        """Get a column definition by name."""
        for column in self.columns:
            if column.name == name:
                return column
        return None

    def get_column_names(self) -> List[str]:
        """Get all column names."""
        return [column.name for column in self.columns]
