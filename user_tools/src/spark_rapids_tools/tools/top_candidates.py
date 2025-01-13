# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
from tabulate import tabulate

from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_tools.utils import Utilities


@dataclass
class TopCandidates:
    """
    Encapsulates the logic to get top candidates from the Qualification report.
    """
    props: dict = field(init=True)
    total_apps: pd.DataFrame = field(init=True)  # Total apps, including failed or skipped
    tools_processed_apps: pd.DataFrame = field(init=True)  # Apps after tools processing and heuristic filtering
    filtered_apps: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)  # Apps after applying filters
    filter_enabled: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        # Filter applications based on categories
        self.filter_enabled = self.props.get('filterEnabled', True)
        self._filter_apps()

    def _has_tools_processed_apps(self) -> bool:
        return not self.tools_processed_apps.empty

    def _get_total_apps_count(self) -> int:
        return len(self.total_apps)

    def _get_successful_apps_count(self) -> int:
        if self._get_total_apps_count() > 0:
            return len(self.total_apps[self.total_apps['Status'] == 'SUCCESS'])
        return 0

    def get_filtered_apps_count(self) -> int:
        """
        Returns the count of filtered applications
        """
        return len(self.filtered_apps)

    def _filter_apps(self) -> None:
        """
        Filters the applications based on the eligible categories (Small/Medium/Large) and total core seconds threshold.
        """
        if not self._has_tools_processed_apps():
            self.filtered_apps = pd.DataFrame(columns=self.tools_processed_apps.columns)
            return

        category_col_name = self.props.get('categoryColumnName')
        eligible_categories = self.props.get('eligibleCategories')

        # Note: `filter_condition` can be a combination of multiple filters
        # Filter based on eligible categories (Small/Medium/Large)
        filter_condition = self.tools_processed_apps[category_col_name].isin(eligible_categories)

        # Filter based on total core seconds threshold
        total_core_sec_col = self.props.get('totalCoreSecCol')
        # Convert the string to int because the parse_config method returns a string
        total_core_sec_threshold = int(self.props.get('totalCoreSecThreshold'))
        total_core_sec_condition = self.tools_processed_apps[total_core_sec_col] > total_core_sec_threshold
        filter_condition = filter_condition & total_core_sec_condition

        # Apply all filter conditions to get top candidate view apps
        self.filtered_apps = self.tools_processed_apps[filter_condition]

    def _generate_output_table(self, output_df: pd.DataFrame) -> str:
        """
        Generic method to generate the output table from the output dataframe
        """
        res_df = self._generate_output_table_internal(output_df)
        if res_df.empty:
            return ''
        # squeeze the header titles if enabled
        squeeze_header_enabled = self.props.get('summaryReport', {}).get('compactWidth', False)
        header_width = self.props.get('summaryReport', {}).get('columnWidth', 0) if squeeze_header_enabled else 0
        formatted_df = Utilities.squeeze_df_header(res_df, header_width) if header_width > 0 else res_df
        return tabulate(formatted_df, headers='keys', tablefmt='psql', floatfmt='.2f')

    def _generate_output_table_internal(self, output_df: pd.DataFrame) -> pd.DataFrame:
        """
        Internal implementation to prepare the output table. This can be overridden by the child classes.
        """
        # Create and append 'Speedup Category Order' column to output_df for sorting order
        speedup_category_order = self.props.get('ineligibleCategory') + self.props.get('eligibleCategories')
        df = output_df.copy()
        df['Speedup Category Order'] = \
            df['Estimated GPU Speedup Category'].map({name: i for i, name in enumerate(speedup_category_order)})
        # Sort columns and select output columns
        output_columns = self.props.get('outputColumns')
        sorting_columns = self.props.get('sortingColumns')
        valid_output_columns = list(df.columns.intersection(output_columns))
        res_df = df.sort_values(by=sorting_columns, ascending=False)[valid_output_columns]
        # this is a bit weird since hardcoding, but we don't want this to have ** for csv output
        if 'Estimated GPU Speedup Category' in res_df:
            res_df.rename(columns={'Estimated GPU Speedup Category': 'Estimated GPU Speedup Category**'},
                          inplace=True)
        return res_df

    def _pre_check_app_processing_status(self, app_name: str) -> (bool, str):
        """
        Checks the application processing status and returns comments based on the results.
        """
        # Check #1: If there are no successful applications from JAR processing
        if self._get_successful_apps_count() == 0:
            return False, f'\n{app_name} tool found no successful applications to process.'
        # Check #2: If filter is enabled and there are no qualified applications after applying the filters
        if self.filter_enabled and self.get_filtered_apps_count() == 0:
            return False, (f'\n{app_name} tool found no qualified applications after applying the filters.'
                           f'\nSee the CSV file for the full report or disable the filters.')
        return True, ''

    def _generate_footnotes(self) -> list:
        """
        Fills in footnotes for the applications
        """
        # 'Config Recommendations' and 'Estimated GPU Speedup Category' columns are available only if there are any
        # recommended apps.
        config_recommendations_path = self.props.get('configRecommendationsPath')
        footnotes = []
        if FSUtil.resource_exists(config_recommendations_path):
            footnotes.append(f'* Config Recommendations can be found in {config_recommendations_path}.')
        footnotes.append('** Estimated GPU Speedup Category assumes the user is using the node type recommended '
                         'and config recommendations with the same size cluster as was used with the CPU side.')
        return footnotes

    def _generate_apps_count_summary(self) -> list:
        """
        Returns a list of counts for total applications, processed applications, and top candidates
        """
        return [['Total applications', self._get_total_apps_count()],
                ['Processed applications', self._get_successful_apps_count()],
                ['Top candidates', self.get_filtered_apps_count()]]

    def generate_summary(self, app_name: str) -> list:
        """
        Generates the summary as:
            - Pre-checks the application processing status and provides comments
            - If pre-checks are successful:
                - Generates the output table
                - Adds footnotes
            - Generates a table with the counts of total applications, processed applications, and top candidates
        """
        report_content = []
        pre_check_status, pre_check_msg = self._pre_check_app_processing_status(app_name)
        if pre_check_status:
            # If pre-checks are successful, generate the table and add footnotes
            output_df = self.filtered_apps if self.filter_enabled else self.tools_processed_apps
            report_content.append(self._generate_output_table(output_df))
            report_content.extend(self._generate_footnotes())
        else:
            report_content.append(pre_check_msg)
        report_content.append(Utils.gen_report_sec_header('Report Summary', hrule=False))
        report_content.append(tabulate(self._generate_apps_count_summary(), colalign=('left', 'right')))
        return report_content
