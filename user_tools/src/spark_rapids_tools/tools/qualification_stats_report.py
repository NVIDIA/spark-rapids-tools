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
        self.logger.info('Reading CSV files completed.')

    def _convert_durations(self) -> None:
        # Convert durations from milliseconds to seconds
        self.unsupported_operators_df[['Stage Duration', 'App Duration']] /= 1000
        self.stages_df[['Stage Task Duration', 'Unsupported Task Duration']] /= 1000

    def _merge_dataframes(self) -> None:
        self.logger.info('Merging dataframes to get stats...')
        # Merge unsupported_operators_df with stages_df on App ID and Stage ID
        merged_df = pd.merge(self.unsupported_operators_df, self.stages_df,
                             on=['App ID', 'Stage ID'])

        agg_unsupported_df = (merged_df.groupby(['App ID', 'SQL ID', 'Unsupported Operator']).agg(
            Count=('Unsupported Operator', 'size'),
            Impacted_Stage_Duration=('Stage Duration', 'sum'),
            App_Duration=('App Duration', 'first'),
            Stage_Task_Duration=('Stage Task Duration', 'sum')
        ).reset_index())

        agg_unsupported_df['% of Stage Duration'] = (
                (agg_unsupported_df['Impacted_Stage_Duration'] /
                 agg_unsupported_df['App_Duration']) * 100).round(3)

        agg_unsupported_df['Supported'] = False
        final_df = agg_unsupported_df.rename(columns={
            'Unsupported Operator': 'Operator',
            'Impacted_Stage_Duration': 'Impacted Stage duration(seconds)',
            'Stage_Task_Duration': 'Stage Task Exec duration(seconds)'
        })
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
