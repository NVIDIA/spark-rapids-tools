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

from spark_rapids_tools.enums import ConditionOperator


@dataclass
class TopCandidates:
    """
    Encapsulates the logic to get top candidates from the Qualification report.
    """
    all_apps_df: pd.DataFrame = field(default=None, init=True)
    unsupported_ops_df: pd.DataFrame = field(default=None, init=True)
    props: dict = field(default=None, init=True)

    def get_candidates(self) -> pd.DataFrame:
        """
        Returns top candidates based on:
        1. Calculates the percentage of total stage duration for unsupported operators for each application
        2. Join the results with all candidates.
        3. Filter the top candidates based on criteria.
        """
        # Define mask to remove rows invalid entries
        mask = self.__create_column_mask(self.props.get('mask'))
        unsupported_ops_df = self.unsupported_ops_df.loc[mask, self.props.get('inputColumns')]

        # Calculate total duration of stages with unsupported operators
        grouping_cols = self.props.get('groupingColumns')
        unsupported_stage_duration = unsupported_ops_df \
            .groupby(grouping_cols.get('max'))['Stage Duration'].max().reset_index() \
            .groupby(grouping_cols.get('sum'))['Stage Duration'].sum().reset_index()

        # Calculate percentage of app duration
        unsupported_stage_duration_percentage = unsupported_stage_duration \
            .assign(unsupported_duration_perc=lambda df: (df['Stage Duration'] * 100) / df['App Duration']) \
            .rename(columns={'unsupported_duration_perc': 'Unsupported Stage Duration Percentage'})

        # Merge results with all app DF and select columns followed by sort
        all_candidates = pd.merge(self.all_apps_df, unsupported_stage_duration_percentage, how='inner',
                                  on=self.props.get('joinColumns'))
        top_candidates = all_candidates[all_candidates.apply(self.__filter_top_candidates, axis=1)]
        return top_candidates[self.props.get('outputColumns')] \
            .sort_values(by=self.props.get('sortingColumns'), ascending=False)

    def __create_column_mask(self, raw_mask: dict) -> bool:
        """
        Creates a column mask based on the provided condition.
        Note: Current implementation is primitive and not a complete AST implementation.
        Example:
            raw_mask = [
              {'columnName': 'colA', 'value': 30, 'operator': 'NOT_EQUAL'},
              {'columnName': 'colB', 'value': 0, 'operator': 'EQUAL'},
              {'columnName': 'colC', 'value': 'dummy', 'operator': 'EQUAL'}
            ]

            mask = True and (colA != 30) and (colB == 0) and (colC == 'dummy')
        """
        mask = True
        for condition in raw_mask:
            column_name, value, operator = condition['columnName'], condition['value'], condition['operator']
            operator_fn = ConditionOperator.get_operator_fn(operator)
            mask &= operator_fn(self.unsupported_ops_df[column_name], value)
        return mask

    def __filter_top_candidates(self, single_row: pd.Series) -> bool:
        """
        Used to create a filter for top candidates based on specified ranges.
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
