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
import fire

from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class SparkQualificationStats:
    """
    Encapsulates the logic to generate the Qualification Stats Report.
    """
    logger: Logger = field(default=None, init=False)
    unsupported_operators_file: str
    stages_file: str
    output_file: str
    unsupported_operators_df: pd.DataFrame = field(default=None, init=False)
    stages_df: pd.DataFrame = field(default=None, init=False)
    result_df: pd.DataFrame = field(default=None, init=False)

    def __init__(self, unsupported_operators_file: str, stages_file: str, output_file: str):
        self.unsupported_operators_file = unsupported_operators_file
        self.stages_file = stages_file
        self.output_file = output_file
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.qualification.stats')

    def load_data(self):
        try:
            self.logger.info('Loading data from CSV files...')
            # Read the CSV files into pandas DataFrames
            self.unsupported_operators_df = pd.read_csv(self.unsupported_operators_file)
            self.stages_df = pd.read_csv(self.stages_file)

            # Convert durations from milliseconds to seconds
            self.unsupported_operators_df['Stage Duration'] = (
                    self.unsupported_operators_df['Stage Duration'] / 1000)
            self.unsupported_operators_df['App Duration'] = (
                    self.unsupported_operators_df['App Duration'] / 1000)
            self.stages_df['Stage Task Duration'] = self.stages_df['Stage Task Duration'] / 1000
            self.stages_df['Unsupported Task Duration'] = (
                    self.stages_df['Unsupported Task Duration'] / 1000)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error loading data: %s', e)

    def merge_dataframes(self):
        try:
            self.logger.info('Merging dataframes...')
            # Merge unsupported_operators_df with stages_df on App ID and Stage ID
            merged_df = pd.merge(self.unsupported_operators_df, self.stages_df,
                                 on=['App ID', 'Stage ID'])

            # # Calculate the percentage of stage duration
            agg_unsupported_df = (merged_df.groupby(['App ID', 'SQL ID', 'Unsupported Operator']).agg(
                Count=('Unsupported Operator', 'size'),
                Impacted_Stage_Duration=('Stage Duration', 'sum'),
                App_Duration=('App Duration', 'sum'),
                Stage_Task_Duration=('Stage Task Duration', 'sum')
            ).reset_index())

            agg_unsupported_df['% of Stage Duration'] = (
                    (agg_unsupported_df['Impacted_Stage_Duration'] /
                     agg_unsupported_df['App_Duration']) * 100)

            # Add the Supported column
            agg_unsupported_df['Supported(Boolean)'] = False

            # Rename columns to match the desired schema
            final_df = agg_unsupported_df.rename(columns={
                'App ID': 'AppId',
                'SQL ID': 'SQLID',
                'Unsupported Operator': 'Operator_Name',
                'Impacted_Stage_Duration': 'Impacted Stage duration',
                'Stage_Task_Duration': 'Stage Task Exec Duration(Seconds)',
                '% of Stage Duration': '% of Stage Duration',
                'Supported': 'Supported(Boolean)'
            })

            # Select the final columns
            final_df = final_df[
                ['AppId', 'SQLID', 'Operator_Name', 'Count', 'Stage Task Exec Duration(Seconds)',
                 'Impacted Stage duration', '% of Stage Duration', 'Supported(Boolean)']]
            self.result_df = final_df
            self.logger.info('Merging dataframes completed.')
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error merging dataframes: %s', e)
            # raise

    def report_qualification_stats(self):
        try:
            self.load_data()
            self.merge_dataframes()
            self.result_df.to_csv(self.output_file, index=False)
            self.logger.info('Results have been saved to %s', self.output_file)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error running analysis: %s', e)


def main(unsupported_operators_file: str, stages_file: str, output_file: str):
    stats = SparkQualificationStats(unsupported_operators_file, stages_file, output_file)
    stats.report_qualification_stats()


if __name__ == '__main__':
    fire.Fire(main)
