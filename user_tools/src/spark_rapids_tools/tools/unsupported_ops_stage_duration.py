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

"""Implementation class for Unsupported Operators Stage Duration logic."""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from spark_rapids_tools.enums import ConditionOperator


@dataclass
class UnsupportedOpsStageDuration:
    """
    Encapsulates the logic to calculate stage duration for unsupported operators
    """
    props: dict = field(default=None, init=True)

    def prepare_apps_with_unsupported_stages(self, all_apps: pd.DataFrame,
                                             unsupported_ops_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform applications to include additional column having stage durations for unsupported operators
        and its percentage of all sql stage durations sum.
        """
        unsupported_stage_duration_percentage = self.__calculate_unsupported_stages_duration(unsupported_ops_df)
        # Note: We might have lost some applications because of masking. Final result should include these
        # applications with unsupported stage duration percentage as 0.0. Thus implying that
        # these applications have no stages with unsupported operator.
        result_df = pd.merge(all_apps, unsupported_stage_duration_percentage, how='left')
        result_col_name = self.props.get('resultColumnName')
        result_df[result_col_name] = result_df[result_col_name].fillna(0)
        # Update the percentage column
        perc_result_col_name = self.props.get('percentResultColumnName')
        # Calculate the percentage of all sql stage durations sum for unsupported operators
        result_df[perc_result_col_name] = np.where(
            result_df['SQL Stage Durations Sum'] != 0,
            result_df[result_col_name] * 100.0 / result_df['SQL Stage Durations Sum'],
            100.0
        )

        return result_df

    def __calculate_unsupported_stages_duration(self, unsupported_ops_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the percentage of all sql stage durations sum for unsupported operators for each application
        """
        # Define mask to remove rows invalid entries
        mask = self.__create_column_mask(unsupported_ops_df)
        unsupported_ops_df = unsupported_ops_df.loc[mask, self.props.get('inputColumns')]

        # Calculate total duration of stages with unsupported operators
        grouping_cols = self.props.get('groupingColumns')
        unsupported_ops_stage_duration = unsupported_ops_df \
            .groupby(grouping_cols.get('max'))['Stage Duration'].max().reset_index() \
            .groupby(grouping_cols.get('sum'))['Stage Duration'].sum().reset_index()

        # Return the calculated unsupported operators stage duration
        return unsupported_ops_stage_duration.rename(columns={'Stage Duration':  self.props.get('resultColumnName')})

    def __create_column_mask(self, unsupported_ops_df: pd.DataFrame) -> bool:
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
        for condition in self.props.get('mask'):
            column_name, value, operator = condition['columnName'], condition['value'], condition['operator']
            operator_fn = ConditionOperator.get_operator_fn(operator)
            mask &= operator_fn(unsupported_ops_df[column_name], value)
        return mask
