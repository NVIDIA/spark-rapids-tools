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
    output_file: str
    unsupported_operators_df: pd.DataFrame
    stages_df: pd.DataFrame
    output_columns: dict
    result_df: pd.DataFrame = field(default=None, init=False)

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.qualification.stats')

    def read_dataframes(self):
        try:
            # Convert durations from milliseconds to seconds
            self.unsupported_operators_df[['Stage Duration', 'App Duration']] /= 1000
            self.stages_df[['Stage Task Duration', 'Unsupported Task Duration']] /= 1000
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error reading dataframe: %s', e)

    def merge_dataframes(self):
        try:
            self.logger.info('Merging dataframes...')
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
                'Unsupported Operator': 'Operator Name',
                'Impacted_Stage_Duration': 'Impacted Stage duration(seconds)',
                'Stage_Task_Duration': 'Stage Task Exec duration(seconds)'
            })
            self.result_df = final_df[self.output_columns.get('columns')].copy()
            self.logger.info('Merging dataframes completed.')
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error merging dataframes: %s', e)

    def report_qualification_stats(self):
        try:
            self.read_dataframes()
            self.merge_dataframes()
            self.result_df.to_csv(self.output_file, index=False)
            self.logger.info('Results have been saved to %s', self.output_file)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error running analysis: %s', e)


def main(unsupported_operators_file: str, stages_file: str, output_file: str):
    unsupported_operators_df = pd.read_csv(unsupported_operators_file)
    stages_df = pd.read_csv(stages_file)
    stats = SparkQualificationStats(unsupported_operators_df=unsupported_operators_df,
                                    stages_df=stages_df,
                                    output_file=output_file)
    stats.report_qualification_stats()


if __name__ == '__main__':
    fire.Fire(main)
