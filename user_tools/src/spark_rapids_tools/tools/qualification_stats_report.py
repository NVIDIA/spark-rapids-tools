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

"""Implementation of the Qualification Stats Report."""


from dataclasses import dataclass, field
from logging import Logger

import pandas as pd

from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


@dataclass
class SparkQualificationStats:
    """
    Encapsulates the logic to generate the Qualification Stats Report.
    """
    logger: Logger = field(default=None, init=False)
    unsupported_operators_df: pd.DataFrame = field(default=None, init=False)
    stages_df: pd.DataFrame = field(default=None, init=False)
    result_df: pd.DataFrame = field(default=None, init=False)
    execs_df: pd.DataFrame = field(default=None, init=False)
    output_columns: dict = field(default=None, init=False)
    qual_output: str = field(default=None, init=True)
    ctxt: ToolContext = field(default=None, init=True)

    def __post_init__(self) -> None:
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.qualification.stats')
        self.output_columns = self.ctxt.get_value('local', 'output', 'files', 'statistics')

    def _read_csv_files(self) -> None:
        self.logger.info('Reading CSV files...')
        if self.qual_output is None:
            qual_output_dir = self.ctxt.get_rapids_output_folder()
        else:
            qual_output_dir = self.qual_output

        unsupported_operator_report_file = self.ctxt.get_value(
            'toolOutput', 'csv', 'unsupportedOperatorsReport', 'fileName')
        rapids_unsupported_operators_file = FSUtil.build_path(
            qual_output_dir, unsupported_operator_report_file)
        self.unsupported_operators_df = pd.read_csv(rapids_unsupported_operators_file)

        stages_report_file = self.ctxt.get_value('toolOutput', 'csv', 'stagesInformation',
                                                 'fileName')
        rapids_stages_file = FSUtil.build_path(qual_output_dir, stages_report_file)
        self.stages_df = pd.read_csv(rapids_stages_file)

        rapids_execs_file = self.ctxt.get_value('toolOutput', 'csv', 'execsInformation',
                                                'fileName')
        self.execs_df = pd.read_csv(FSUtil.build_path(qual_output_dir, rapids_execs_file))
        self.logger.info('Reading CSV files completed.')

    def _convert_durations(self) -> None:
        # Convert durations from milliseconds to seconds
        self.unsupported_operators_df[['Stage Duration', 'App Duration']] /= 1000
        self.stages_df[['Stage Task Duration', 'Unsupported Task Duration']] /= 1000

    def _preprocess_dataframes(self) -> None:
        self.logger.info('Preprocessing dataframes...')

        # Filter out 'WholeStageCodegen' operators as the child operators are already included
        # in the other rows
        self.execs_df = self.execs_df[
            ~self.execs_df['Exec Name'].str.startswith('WholeStageCodegen')]

        # Split 'Exec Stages' and explode the list into separate rows so that the stageID
        # from this dataframe can be matched with the stageID of stages dataframe
        self.execs_df['Exec Stages'] = self.execs_df['Exec Stages'].str.split(':')
        self.execs_df = self.execs_df.explode('Exec Stages').dropna(subset=['Exec Stages'])
        self.execs_df['Exec Stages'] = self.execs_df['Exec Stages'].astype(int)

        # Remove duplicate 'Stage ID' rows and rename some columns so that join on dataframes
        # can be done easily
        self.stages_df = self.stages_df.drop_duplicates(subset=['App ID', 'Stage ID'])
        self.stages_df.rename(columns={'Stage Task Duration': 'StageTaskDuration'}, inplace=True)
        self.execs_df.rename(columns={'Exec Name': 'Operator'}, inplace=True)
        self.unsupported_operators_df.rename(columns={'Unsupported Operator': 'Operator'},
                                             inplace=True)
        self.logger.info('Preprocessing dataframes completed.')

    def _merge_dataframes(self) -> None:
        self.logger.info('Merging dataframes to get stats...')
        self._preprocess_dataframes()

        # Merge execs_df with stages_df
        merged_df = self.execs_df.merge(self.stages_df, left_on=['App ID', 'Exec Stages'],
                                        right_on=['App ID', 'Stage ID'], how='left')

        # Merge with unsupported_operators_df to find unsupported operations
        merged_df = merged_df.merge(self.unsupported_operators_df,
                                    on=['App ID', 'SQL ID', 'Stage ID', 'Operator'],
                                    how='left', indicator=True)
        merged_df['Supported'] = merged_df['_merge'] == 'left_only'
        merged_df.drop(columns=['_merge', 'Exec Stages'], inplace=True)

        # Calculate total duration by summing unique stages per SQLID
        total_duration_df = merged_df.drop_duplicates(subset=['App ID', 'SQL ID', 'Stage ID']) \
            .groupby(['App ID', 'SQL ID'])['StageTaskDuration'] \
            .sum().reset_index().rename(columns={'StageTaskDuration': 'TotalSQLDuration'})

        merged_df = merged_df.merge(total_duration_df, on=['App ID', 'SQL ID'], how='left')

        # Mark unique stage task durations
        merged_df['Unique StageTaskDuration'] = ~merged_df.duplicated(
            ['App ID', 'SQL ID', 'Operator', 'Stage ID', 'Supported'])
        merged_df['Adjusted StageTaskDuration'] = (merged_df['StageTaskDuration'] *
                                                   merged_df['Unique StageTaskDuration'])

        # Aggregate data
        final_df = merged_df.groupby(['App ID', 'SQL ID', 'Operator', 'Supported']).agg({
            'Adjusted StageTaskDuration': 'sum',
            'Stage ID': 'count'
        }).reset_index().rename(columns={'Stage ID': 'Count',
                                         'Adjusted StageTaskDuration': 'StageTaskDuration'})

        # Merge total duration and calculate percentage
        final_df = final_df.merge(total_duration_df, on=['App ID', 'SQL ID'], how='left')
        final_df['% of Total SQL Duration'] = (
                final_df['StageTaskDuration'] / final_df['TotalSQLDuration'] * 100)

        # Rename columns
        final_df.rename(columns={
            'StageTaskDuration': 'Stage Task Exec Duration(s)',
            'TotalSQLDuration': 'Total SQL Duration(s)'
        }, inplace=True)
        self.result_df = final_df[self.output_columns.get('columns')].copy()
        self.logger.info('Merging stats dataframes completed.')

    def _write_results(self) -> None:
        self.logger.info('Writing stats results...')
        result_output_dir = self.ctxt.get_output_folder()
        outputfile_path = self.ctxt.get_value('local', 'output', 'files', 'statistics', 'name')
        output_file = FSUtil.build_path(result_output_dir, outputfile_path)
        self.result_df.to_csv(output_file, float_format='%.2f', index=False)
        self.logger.info('Results have been saved to %s', output_file)

    def report_qualification_stats(self) -> None:
        """
        Reports qualification stats by reading qual tool output CSV files

        If an error occurs, the caller should handle the exception.
        """
        self._read_csv_files()
        self._convert_durations()
        self._merge_dataframes()
        self._write_results()
