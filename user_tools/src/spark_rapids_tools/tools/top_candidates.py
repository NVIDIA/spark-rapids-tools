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


@dataclass
class TopCandidates:
    """
    Encapsulates the logic to get top candidates from the Qualification report.
    """
    props: dict = field(default=None, init=True)

    def filter_apps(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        """
        Generic method to filter applications based on criteria
        """
        category_col_name = self.props.get('categoryColumnName')
        eligible_categories = self.props.get('eligibleCategories')
        # Filter applications based on categories
        return all_apps[all_apps[category_col_name].isin(eligible_categories)]

    def prepare_output(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        """
        Generic method to transform applications for the output
        """
        output_columns = self.props.get('outputColumns')
        sorting_columns = self.props.get('sortingColumns')
        # Sort columns and select output columns
        return all_apps.sort_values(by=sorting_columns, ascending=False)[output_columns]
