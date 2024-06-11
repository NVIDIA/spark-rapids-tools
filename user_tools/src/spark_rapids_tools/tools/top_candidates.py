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
import logging


@dataclass
class TopCandidates:
    """
    Encapsulates the logic to get top candidates from the Qualification report.
    """
    props: dict = field(default=None, init=True)
    logger = logging.getLogger(__name__)


    def filter_apps(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        """
        Generic method to filter applications based on criteria
        """
        self.logger.warning('after filter apps before %s' + ','.join(list(all_apps.columns.values)))
        category_col_name = self.props.get('categoryColumnName')
        eligible_categories = self.props.get('eligibleCategories')
        self.logger.warning('TOM in filter apps %s' + ','.join(eligible_categories))
        self.logger.warning('after filter apps %s' + ','.join(list(all_apps.columns.values)))
        # Filter applications based on categories
        return all_apps[all_apps[category_col_name].isin(eligible_categories)]

    def prepare_output(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        """
        Generic method to transform applications for the output
        """
        self.logger.warning('after prepare output before %s' + ','.join(list(all_apps.columns.values)))
        output_columns = self.props.get('outputColumns')
        self.logger.warning('TOM in after output columns %s' + ','.join(output_columns))
        sorting_columns = self.props.get('sortingColumns')
        valid_output_columns = list(all_apps.columns.intersection(output_columns))
        self.logger.warning('TOM in after validate %s' + ','.join(valid_output_columns))
        valid_sorting_columns = list(all_apps.columns.intersection(sorting_columns))
        # Sort columns and select output columns
        final_apps = all_apps.sort_values(by=valid_sorting_columns, ascending=False)[valid_output_columns]
        self.logger.warning('after final apps before %s' + ','.join(list(final_apps.columns.values)))
        return final_apps 
