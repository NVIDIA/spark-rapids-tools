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
from logging import Logger

import pandas as pd

from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.enums import ConditionOperator


@dataclass
class UnsupportedOpsStageDuration:
    """
    Encapsulates the logic to calculate stage duration for unsupported operators
    """
    props: dict = field(default=None, init=True)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.unsupported_ops_stage_duration'),
                           init=False)

    def prepare_apps_with_unsupported_stages(self, all_apps: pd.DataFrame,
                                             unsupported_ops_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform applications to include additional column having stage durations percentage
        of unsupported operators.
        """
        unsupported_stage_duration_percentage = self.__calculate_unsupported_stages_duration(unsupported_ops_df)
        # Note: We might have lost some applications because of masking. Final result should include these
        # applications with unsupported stage duration percentage as 0.0. Thus implying that
        # these applications have no stages with unsupported operator.
        return pd.merge(all_apps, unsupported_stage_duration_percentage, how='left', on='App ID')

    def __calculate_unsupported_stages_duration(self, unsupported_ops_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the percentage of total stage duration for unsupported operators for each application
        """
        # Fetch the result column names
        result_col_name = self.props.get('resultColumnName')
        perc_result_col_name = self.props.get('percentResultColumnName')
        total_stage_duration_col_name = self.props.get('totalStageDurationColumnName')

        try:
            total_stage_duration_df = unsupported_ops_df[['App ID', total_stage_duration_col_name]].drop_duplicates()
            # Define mask to remove rows invalid entries
            mask = self.__create_column_mask(unsupported_ops_df)
            unsupported_ops_df = unsupported_ops_df.loc[mask, self.props.get('inputColumns')]

            # Calculate total duration of stages with unsupported operators
            grouping_cols = self.props.get('groupingColumns')
            unsupported_ops_stage_duration = unsupported_ops_df \
                .groupby(grouping_cols.get('max'))['Stage Duration'].max().reset_index() \
                .groupby(grouping_cols.get('sum'))['Stage Duration'].sum().reset_index()

            # Rename the 'Stage Duration' column to 'Unsupported Operators Stage Duration' and fill NaN with 0
            unsupported_ops_stage_duration.rename(columns={'Stage Duration': result_col_name}, inplace=True)

            # Create result df with App ID and unsupported ops stage duration
            result_df = unsupported_ops_stage_duration[['App ID', result_col_name]]

            # Merge the result with total stage duration for all apps and fill NaN with 0
            result_df = pd.merge(total_stage_duration_df, result_df, how='left', on='App ID')
            result_df.fillna(0, inplace=True)

            # Calculate percentage of total stage duration for unsupported operators
            result_df[perc_result_col_name] =\
                result_df[result_col_name] * 100.0 / result_df[total_stage_duration_col_name]
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error while calculating unsupported stages duration. Reason - %s:%s',
                              type(e).__name__, e)
            result_df = pd.DataFrame(columns=['App ID', total_stage_duration_col_name, result_col_name,
                                              perc_result_col_name])
        return result_df

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
