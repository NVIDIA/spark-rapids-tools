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
Module that contains definitions that do not include classes from the API.
This avoids cyclic imports in the API module.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import pandas as pd

from spark_rapids_tools.utils import Utilities
from spark_rapids_tools.utils.data_utils import LoadDFResult


@dataclass
class APIUtils(object):
    """
    A utility class that provides methods for string/pandas
    """
    @staticmethod
    def scala_to_pd_dtype(scala_type: str) -> str:
        """
        Converts a Scala data type string to its corresponding Pandas-compatible type.
        Note that it is important to use Pandas Nullable type in order to support null data
        passed from the core-tools. For example, Pandas cannot use 'int64' for an integer column if
        it has missing values. It always expect it to be a NaN which might not be the case for
        Scala.

        :param scala_type: The Scala data type string (e.g., 'Int', 'String', 'Double').
        :return: (str) The Pandas-compatible data type string (e.g., 'int64', 'object', 'float64').
        """
        scala_to_pandas_map = {
            'Int': 'Int64',         # Nullable integer
            'Long': 'Int64',        # Both Scala Int and Long map to int64 in Pandas for typical usage.
            'Float': 'float32',     # Float is already nullable (supports NaN)
            'Double': 'float64',    # Float is already nullable (supports NaN)
            'String': 'string',     # Pandas nullable string dtype
            'Boolean': 'boolean',   # Pandas nullable boolean dtype
            'Timestamp': 'datetime64[ns]',
            'Date': 'datetime64[ns]',  # Pandas represents both Date and Timestamp as datetime64[ns].
            'Decimal': 'object',  # Pandas may not have a direct equivalent for Decimal, so 'object' is used.
            # Add more mappings for other Scala types as needed
        }
        return scala_to_pandas_map.get(scala_type, 'object')  # Default to object for unknown types.

    @staticmethod
    def cols_to_camel_case(col_names: List[str]) -> Dict[str, str]:
        """
        Map the column names to camelCase.
        :param col_names: The list of column names to map.
        :return: A dictionary mapping the original column names to their camelCase equivalents.
        """
        return {col: Utilities.str_to_camel(col) for col in col_names}


@dataclass
class LoadCombinedRepResult(object):
    """
    A dataclass to hold the result of loading raw files.
    :param res_id: An ID of the container. This can be used as way to track different operations and
           log them in a human friendly way.
    :param _reports: A Private field mapping [report_id: str, dataFrame: pd.DataFrame]. Each dataframe is
           the combined dataframe of all per-app tools output.
    :param _failed_loads: A dictionary mapping the appIDs to the failed reports. Each entry is of the structure
           [app_id: str, tuple(report_id: str, error: str)] in order to give details on the root
           cause of each failure in-case the caller needs to process those failures.
    """
    res_id: str
    _reports: Dict[str, pd.DataFrame] = field(init=False, default_factory=dict)
    _failed_loads: Dict[str, List[Tuple[str, str]]] = field(init=False, default_factory=dict)

    @property
    def reports(self) -> Dict[str, pd.DataFrame]:
        """
        Get the reports loaded from the CSV files.
        :return: A dictionary mapping report IDs to their DataFrames.
        """
        return self._reports

    @property
    def failed_loads(self) -> Dict[str, List[Tuple[str, str]]]:
        """
        Get the failed loads.
        :return: A dictionary mapping app IDs to a list of tuples containing report ID and error message.
        """
        return self._failed_loads

    def append_success(self, report_id: str, df_res: LoadDFResult) -> None:
        """
        Append a successful report to the csv_files.
        :param report_id: The unique identifier for the report.
        :param df_res: The result of loading a DataFrame.
        """
        self._reports[report_id] = df_res.data

    def append_failure(self, app_id: str, report_id: str, load_excep: Optional[Exception]) -> None:
        """
        Append a report to the csv_files or failed_loads based on the success of the load.
        :param app_id: The unique identifier for the application.
        :param report_id: The unique identifier for the report.
        :param load_excep: The exception raised during the loading of the report.
        """
        self._failed_loads.setdefault(app_id, []).append((report_id, str(load_excep)))
