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

"""
QualOutputFileReader class for reading qualification tool output files.

This class provides a clean interface for reading qualification output files
from the new per-app structure using QualCoreTableLoader.
"""

import json
from pathlib import Path
from typing import Optional

import pandas as pd

from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_tools.tools.core import QualCoreTableLoader
from spark_rapids_tools.utils.data_utils import DataUtils


class QualOutputFileReader:
    """
    Reader class for qualification tool output files.

    This class handles reading from the new qualification output structure:
    - Global files: output_dir/qual_core_output/<filename>
    - Per-app files: output_dir/qual_core_output/qual_metrics/application_<id>/<filename>
    """

    def __init__(self, output_directory: str = None):
        """
        Initialize the reader with the qualification output directory.

        Args:
            output_directory: Path to the qualification tool output directory
        """
        self.output_directory = output_directory
        self.table_loader = QualCoreTableLoader()

    def update_output_directory(self, output_directory: str):
        """
        Update the output directory for the reader.

        Args:
            output_directory: New path to the qualification tool output directory
        """
        self.output_directory = output_directory

    def read_table_by_label(self, label: str, file_format: str = 'csv') -> pd.DataFrame:
        """
        Read qualification tool output files by table label.

        Args:
            label: The table label from the YAML configuration
            file_format: File format to read ('csv' or 'json')

        Returns:
            DataFrame containing the file contents, or empty DataFrame if file not found
        """
        if self.output_directory is None:
            raise ValueError("Output directory not set. Call update_output_directory() first.")

        table_def = self.table_loader.get_table_by_label(label)

        if table_def.scope == 'global':
            return self._read_global_file(table_def, file_format)
        elif table_def.scope == 'per-app':
            return self._read_per_app_files(table_def, file_format)
        else:
            raise ValueError(f'Unknown table scope: {table_def.scope} for label: {label}')

    def _read_global_file(self, table_def, file_format: str) -> pd.DataFrame:
        """Read a global scope file."""
        base_path = FSUtil.build_path(self.output_directory, 'qual_core_output')
        report_file_path = FSUtil.build_path(base_path, table_def.file_name)

        if file_format.lower() == 'json' or table_def.file_format == 'JSON':
            try:
                with open(report_file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                return pd.json_normalize(json_data) if json_data else pd.DataFrame()
            except Exception:
                return pd.DataFrame()
        else:
            try:
                return DataUtils.read_dataframe(report_file_path)
            except Exception:
                return pd.DataFrame()

    def _read_per_app_files(self, table_def, file_format: str) -> pd.DataFrame:
        """Read and combine per-app scope files."""
        qual_core_path = FSUtil.build_path(self.output_directory, 'qual_core_output')
        qual_metrics_path = FSUtil.build_path(qual_core_path, 'qual_metrics')

        combined_dfs = []
        try:
            metrics_dir = Path(qual_metrics_path)

            if not metrics_dir.exists():
                return pd.DataFrame()

            for app_dir in metrics_dir.iterdir():
                if app_dir.is_dir():
                    app_id = app_dir.name  # Extract App ID from directory name
                    app_file_path = app_dir / table_def.file_name

                    if app_file_path.exists():
                        try:
                            if file_format.lower() == 'json' or table_def.file_format == 'JSON':
                                with open(app_file_path, 'r', encoding='utf-8') as f:
                                    json_data = json.load(f)
                                if json_data:
                                    app_df = pd.json_normalize(json_data)
                                    combined_dfs.append(app_df)
                            else:
                                app_df = DataUtils.read_dataframe(app_file_path)
                                # Add App ID column as the first column to maintain conformity
                                app_df.insert(0, 'App ID', app_id)
                                combined_dfs.append(app_df)
                        except Exception:
                            continue  # Skip files that can't be read

            # Combine all DataFrames
            if combined_dfs:
                return pd.concat(combined_dfs, ignore_index=True)
            else:
                return pd.DataFrame()

        except Exception:
            return pd.DataFrame()

    def list_available_tables(self) -> list:
        """
        List all available table labels that can be read.

        Returns:
            List of available table labels
        """
        return self.table_loader.list_table_labels()
