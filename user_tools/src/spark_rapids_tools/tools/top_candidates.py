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

"""Implementation class for Top Candidates logic."""

from dataclasses import dataclass, field

import pandas as pd

from spark_rapids_tools.utils import Utilities


@dataclass
class TopCandidates:
    """
    Encapsulates the logic to get top candidates from the Qualification report.
    """
    props: dict = field(default_factory=dict, init=True)
    filter_enabled: bool = field(default=True, init=True)
    all_apps: pd.DataFrame = field(default=None, init=True)
    header_width: int = field(default=0, init=True)
    filtered_apps: pd.DataFrame = field(default=None, init=False)

    def __post_init__(self) -> None:
        # Filter applications based on categories
        category_col_name = self.props.get('categoryColumnName')
        eligible_categories = self.props.get('eligibleCategories')
        self.filtered_apps = self.all_apps[self.all_apps[category_col_name].isin(eligible_categories)]

    def get_all_apps_count(self) -> int:
        """
        Returns the count of all applications
        """
        return len(self.all_apps)

    def get_filtered_apps_count(self) -> int:
        """
        Returns the count of filtered applications
        """
        return len(self.filtered_apps)

    def prepare_output(self) -> pd.DataFrame:
        """
        Generic method to transform applications for the output
        """
        if self.filter_enabled:
            res_df = self._prepare_output_internal(self.filtered_apps)
        else:
            res_df = self._prepare_output_internal(self.all_apps)
        if self.header_width == 0:
            return res_df
        # squeeze the header titles if enabled
        return Utilities.squeeze_df_header(res_df, self.header_width)

    def _prepare_output_internal(self, apps_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generic method to transform applications for the output
        """
        output_columns = self.props.get('outputColumns')
        sorting_columns = self.props.get('sortingColumns')
        valid_output_columns = list(apps_df.columns.intersection(output_columns))
        valid_sorting_columns = list(apps_df.columns.intersection(sorting_columns))
        # Sort columns and select output columns
        res_df = apps_df.sort_values(by=valid_sorting_columns, ascending=False)[valid_output_columns]
        # this is a bit weird since hardcoding but we don't want this to have ** for csv output
        if 'Estimated GPU Speedup Category' in res_df:
            res_df.rename(columns={'Estimated GPU Speedup Category': 'Estimated GPU Speedup Category**'},
                          inplace=True)
        return res_df
