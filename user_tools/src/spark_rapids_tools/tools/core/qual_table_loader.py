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

"""Qualification Output Related Table Loader"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from spark_rapids_pytools.common.prop_manager import YAMLPropertiesContainer
from spark_rapids_pytools.common.utilities import Utils
from .qual_table_definitions import QualCoreTableDef, QualCoreColumnDef


@dataclass
class QualCoreTableLoader:
    """Loads and manages qualification table definitions from YAML configuration files."""
    yaml_file_path: Optional[str] = None
    _table_definitions: Optional[List[QualCoreTableDef]] = None

    def __post_init__(self):
        """Initialize the loader with proper yaml_file_path and convert to Path object."""
        if self.yaml_file_path is None:
            self.yaml_file_path = Utils.resource_path('generated_files/core/reports/qualOutputTable.yaml')
        self.yaml_file_path = Path(self.yaml_file_path)

    def load_table_definitions(self) -> List[QualCoreTableDef]:
        """Load table definitions from the YAML file."""
        if self._table_definitions is not None:
            return self._table_definitions

        try:
            yaml_container = YAMLPropertiesContainer(prop_arg=self.yaml_file_path)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f'qualOutputTable.yaml not found at: {self.yaml_file_path}') from exc

        qual_table_definitions = yaml_container.get_value('qualTableDefinitions')

        if qual_table_definitions is None:
            raise ValueError('YAML file must contain \'qualTableDefinitions\' key')

        table_definitions = []
        for table_data in qual_table_definitions:
            table_def = self._create_table_definition(table_data)
            table_definitions.append(table_def)

        self._table_definitions = table_definitions
        return self._table_definitions

    def _create_table_definition(self, table_data: Dict) -> QualCoreTableDef:
        """Create a QualCoreTableDef from dictionary data."""
        required_fields = ['label', 'description', 'fileName', 'scope', 'columns']
        for field in required_fields:
            if field not in table_data:
                raise ValueError(f'Missing required field \'{field}\' in table definition')

        columns = []
        for column_data in table_data['columns']:
            column_def = self._create_column_definition(column_data)
            columns.append(column_def)

        return QualCoreTableDef(
            label=table_data['label'],
            description=table_data['description'].strip() if table_data['description'] else '',
            file_name=table_data['fileName'],
            scope=table_data['scope'],
            columns=columns,
            file_format=table_data.get('fileFormat')  # Optional field
        )

    def _create_column_definition(self, column_data: Dict) -> QualCoreColumnDef:
        """Create a QualCoreColumnDef from dictionary data."""
        required_fields = ['name', 'dataType', 'description']
        for field in required_fields:
            if field not in column_data:
                raise ValueError(f'Missing required field \'{field}\' in column definition')

        return QualCoreColumnDef(
            name=column_data['name'],
            data_type=column_data['dataType'],
            description=column_data['description'].strip() if column_data['description'] else ''
        )

    def get_table_by_label(self, label: str) -> Optional[QualCoreTableDef]:
        """Get a table definition by its label."""
        tables = self.load_table_definitions()
        for table in tables:
            if table.label == label:
                return table
        return None

    def get_table_by_filename(self, filename: str) -> Optional[QualCoreTableDef]:
        """Get a table definition by its filename."""
        tables = self.load_table_definitions()
        for table in tables:
            if table.file_name == filename:
                return table
        return None

    def get_tables_by_scope(self, scope: str) -> List[QualCoreTableDef]:
        """Get all table definitions for a specific scope."""
        tables = self.load_table_definitions()
        return [table for table in tables if table.scope == scope]

    def reload(self) -> None:
        """Force reload of the table definitions from the YAML file."""
        self._table_definitions = None
