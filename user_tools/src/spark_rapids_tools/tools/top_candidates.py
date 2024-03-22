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
from typing import Optional
from functools import partial

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
        filtered_apps = all_apps[all_apps.apply(self.__filter_single_row, axis=1)]
        # Select output columns and sort
        output_columns = self.props.get('outputColumns')
        sorting_columns = self.props.get('sortingColumns')
        return filtered_apps[output_columns].sort_values(by=sorting_columns, ascending=False)

    def __filter_single_row(self, single_row: pd.Series) -> bool:
        """
        Used to create a filter for based on specified ranges.
        Example:
        self.props['ranges'] = [
            {'columnName': 'colA', 'lowerBound': 18, 'upperBound': 30},
            {'columnName': 'colB', 'lowerBound': 70, 'upperBound': 100}
        ]
        single_row = pd.Series({'colA': 25, 'colB': 85})
        The function will return True because the colA (25) is within the range (18-30)
        and the colB (85) is within the range (70-100).
        """
        for criteria in self.props.get('ranges'):
            col_value = single_row[criteria.get('columnName')]
            if not criteria.get('lowerBound') <= col_value <= criteria.get('upperBound'):
                return False
        return True

    def prepare_output(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        """
        Generic method to transform applications for the output
        """
        output_props = self.props.get('output')

        # Function to remap column values based on recommended ranges
        def remap_column(col_value, recommended_ranges: dict) -> Optional[str]:
            for s_range in recommended_ranges:
                if s_range['lowerBound'] <= col_value < s_range['upperBound']:
                    return s_range['title']
            return None

        # Iterate over each entry and apply remapping to respective columns
        for remap_entry in output_props.get('remap', []):
            column_name = remap_entry.get('columnName')
            recommendation_ranges = remap_entry.get('recommendationRanges')
            remap_func = partial(remap_column, recommended_ranges=recommendation_ranges)
            all_apps[column_name] = all_apps[column_name].apply(remap_func)
        return all_apps[output_props.get('columns', [])]
