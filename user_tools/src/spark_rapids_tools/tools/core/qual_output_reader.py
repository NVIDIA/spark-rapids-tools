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

"""Qualification Output Related File Reader"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, Dict

import pandas as pd

from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_tools import CspPathT
from spark_rapids_tools.tools.core import QualCoreTableLoader
from spark_rapids_tools.utils.data_utils import DataUtils


@dataclass
class QualOutputFileReader:
    """Reader for qualification tool output files with global and per-app file support."""
    output_directory: Optional[Union[str, CspPathT]] = None
    table_loader: QualCoreTableLoader = field(default_factory=QualCoreTableLoader)

    def update_output_directory(self, output_directory: Union[str, CspPathT]):
        """
        Update the output directory for the reader.

        Args:
            output_directory: New path to the qualification tool output directory (str or CspPathT)
        """
        self.output_directory = output_directory

    def read_table_by_label(self,
                            label: str,
                            file_format: str = 'csv',
                            read_csv_kwargs: Optional[dict] = None) -> pd.DataFrame:
        """Read qualification tool output files by table label."""
        if self.output_directory is None:
            raise ValueError('Output directory not set. Call update_output_directory() first.')

        table_def = self.table_loader.get_table_by_label(label)

        if table_def.scope == 'global':
            return self._read_global_file(table_def, file_format, read_csv_kwargs)
        if table_def.scope == 'per-app':
            return self._read_per_app_files(table_def, file_format, read_csv_kwargs)
        raise ValueError(f'Unknown table scope: {table_def.scope} for label: {label}')

    def _read_global_file(self,
                          table_def,
                          file_format: str,
                          read_csv_kwargs: Optional[dict] = None) -> pd.DataFrame:
        """Read a global scope file."""
        base_path = FSUtil.build_path(self.output_directory, 'qual_core_output')
        report_file_path = FSUtil.build_path(base_path, table_def.file_name)

        if file_format.lower() == 'json' or table_def.file_format == 'JSON':
            # Check if this is cluster information JSON and needs column mapping
            map_columns = None
            if table_def.label == 'clusterInfoJSONReport':
                map_columns = self._get_cluster_info_json_to_csv_mapping()

            result = DataUtils.load_pd_df_from_json(report_file_path, map_columns=map_columns)
            return result.data if result.success else pd.DataFrame()
        try:
            result = DataUtils.load_pd_df(report_file_path, read_csv_kwargs=read_csv_kwargs)
            return result.data if result.success else pd.DataFrame()
        except Exception:  # pylint: disable=broad-except
            return pd.DataFrame()

    def _read_per_app_files(self,
                            table_def,
                            file_format: str,
                            read_csv_kwargs: Optional[dict] = None) -> pd.DataFrame:
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
                                # In case of cluster information JSON, we need to currently
                                # map the JSON columns to the expected CSV column name. This patch
                                # is temporary and will be removed once the underlying cluster inference
                                # code is updated to use the newer JSON column names.
                                map_columns = None
                                if table_def.label == 'clusterInfoJSONReport':
                                    map_columns = self._get_cluster_info_json_to_csv_mapping()
                                result = DataUtils.load_pd_df_from_json(str(app_file_path), map_columns=map_columns)
                                if result.success and not result.data.empty:
                                    app_df = result.data
                                    combined_dfs.append(app_df)
                            else:
                                result = DataUtils.load_pd_df(str(app_file_path), read_csv_kwargs=read_csv_kwargs)
                                if result.success:
                                    app_df = result.data
                                    # Add App ID column as the first column to maintain conformity
                                    app_df.insert(0, 'App ID', app_id)
                                    combined_dfs.append(app_df)
                        except Exception:  # pylint: disable=broad-except
                            continue  # Skip files that can't be read

            # Combine all DataFrames
            if combined_dfs:
                return pd.concat(combined_dfs, ignore_index=True)
            return pd.DataFrame()

        except Exception:  # pylint: disable=broad-except
            return pd.DataFrame()

    def read_raw_metric_per_app_files(self,
                                      file_name: str,
                                      read_csv_kwargs: Optional[dict] = None) -> Dict[str, pd.DataFrame]:
        """Read a specific raw metric file from each application's raw_metrics directory."""
        metrics = {}
        try:
            qual_core_path = FSUtil.build_path(self.output_directory, 'qual_core_output')
            raw_metrics_path = FSUtil.build_path(qual_core_path, 'raw_metrics')

            if not Path(raw_metrics_path).exists():
                return metrics

            raw_metrics_dir = Path(raw_metrics_path)

            for app_dir in raw_metrics_dir.iterdir():
                if app_dir.is_dir():
                    app_id_str = app_dir.name  # Extract App ID from directory name
                    metric_file_path = app_dir / file_name

                    try:
                        if metric_file_path.exists():
                            result = DataUtils.load_pd_df(str(metric_file_path), read_csv_kwargs=read_csv_kwargs)
                            if result.success:
                                metrics[app_id_str] = result.data
                            else:
                                metrics[app_id_str] = pd.DataFrame()
                        else:
                            metrics[app_id_str] = pd.DataFrame()
                    except Exception:  # pylint: disable=broad-except
                        # Some apps may not have the given metrics file, maintain consistency
                        metrics[app_id_str] = pd.DataFrame()

        except Exception:  # pylint: disable=broad-except
            pass

        return metrics

    def _get_cluster_info_json_to_csv_mapping(self) -> dict:
        """Get the mapping from JSON column names (after json_normalize) to CSV column names
        for cluster information files.
        This is a temporary mapping that will be removed once the underlying cluster inference
        code is updated to use the newer JSON column names. All expected column names
        have been added to the mapping."""
        return {
            'appId': 'App ID',
            'appName': 'App Name',
            'eventLogPath': 'Event Log',
            'sourceClusterInfo.vendor': 'Vendor',
            'sourceClusterInfo.driverHost': 'Driver Host',
            'sourceClusterInfo.clusterId': 'Cluster Id',
            'sourceClusterInfo.clusterName': 'Cluster Name',
            'sourceClusterInfo.workerNodeType': 'Worker Node Type',
            'sourceClusterInfo.driverNodeType': 'Driver Node Type',
            'sourceClusterInfo.numWorkerNodes': 'Num Worker Nodes',
            'sourceClusterInfo.numExecsPerNode': 'Num Executors Per Node',
            'sourceClusterInfo.numExecutors': 'Num Executors',
            'sourceClusterInfo.executorHeapMemory': 'Executor Heap Memory',
            'sourceClusterInfo.dynamicAllocationEnabled': 'Dynamic Allocation Enabled',
            'sourceClusterInfo.dynamicAllocationMaxExecutors': 'Dynamic Allocation Max Executors',
            'sourceClusterInfo.dynamicAllocationMinExecutors': 'Dynamic Allocation Min Executors',
            'sourceClusterInfo.dynamicAllocationInitialExecutors': 'Dynamic Allocation Initial Executors',
            'sourceClusterInfo.coresPerExecutor': 'Cores Per Executor',
            'recommendedClusterInfo.driverNodeType': 'Recommended Driver Node Type',
            'recommendedClusterInfo.workerNodeType': 'Recommended Worker Node Type',
            'recommendedClusterInfo.numExecutors': 'Recommended Num Executors',
            'recommendedClusterInfo.numWorkerNodes': 'Recommended Num Worker Nodes',
            'recommendedClusterInfo.coresPerExecutor': 'Recommended Cores Per Executor',
            'recommendedClusterInfo.gpuDevice': 'Recommended GPU Device',
            'recommendedClusterInfo.numGpusPerNode': 'Recommended Num GPUs Per Node',
            'recommendedClusterInfo.vendor': 'Recommended Vendor',
            'recommendedClusterInfo.dynamicAllocationEnabled': 'Recommended Dynamic Allocation Enabled',
            'recommendedClusterInfo.dynamicAllocationMaxExecutors': 'Recommended Dynamic Allocation Max Executors',
            'recommendedClusterInfo.dynamicAllocationMinExecutors': 'Recommended Dynamic Allocation Min Executors',
            'recommendedClusterInfo.dynamicAllocationInitialExecutors':
                'Recommended Dynamic Allocation Initial Executors'
        }
