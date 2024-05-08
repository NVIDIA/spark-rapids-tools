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

"""Implementation class for Additional Heuristics logic."""

import os
import re
from dataclasses import dataclass, field
from logging import Logger
from pathlib import Path

import pandas as pd

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.tools.model_xgboost import find_paths, RegexPattern


@dataclass
class AdditionalHeuristics:
    """
    Encapsulates the logic to apply additional heuristics to skip applications.
    """
    logger: Logger = field(default=None, init=False)
    props: JSONPropertiesContainer = field(default=None, init=False)
    output_dir: str = field(default=None, init=False)

    def __init__(self, props: dict, output_dir: str):
        self.props = JSONPropertiesContainer(props, file_load=False)
        self.output_dir = output_dir
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.{self.__class__.__name__}')

    def _apply_heuristics(self) -> pd.DataFrame:
        """
        Apply additional heuristics to applications to determine if they can be accelerated on GPU.
        """
        profile_list = find_paths(
            self.output_dir,
            RegexPattern.rapids_profile.match,
            return_directories=True,
        )
        if not profile_list:
            self.logger.warning('No RAPIDS profiles found in output directory: %s', self.output_dir)
            return pd.DataFrame()

        app_id_paths = find_paths(profile_list[0], RegexPattern.app_id.match, return_directories=True)
        result_arr = []
        if not app_id_paths:
            self.logger.warning('Skipping empty profile: %s', profile_list[0])
        else:
            for app_id_path in app_id_paths:
                try:
                    app_info_path = os.path.join(app_id_path, self.props.get_value('appInfo', 'fileName'))
                    app_info = pd.read_csv(app_info_path)
                    app_name = app_info['appName'].values[0]
                    app_id = Path(app_id_path).name
                    # Apply heuristics and determine if the application should be skipped.
                    # Note: `should_skip` flag can be a combination of multiple heuristic checks.
                    should_skip = self.heuristics_based_on_spills(app_id_path)
                    result_arr.append([app_name, app_id, should_skip])
                except Exception as e:  # pylint: disable=broad-except
                    self.logger.error('Error occurred while applying additional heuristics. '
                                      'Reason - %s:%s', type(e).__name__, e)
        return pd.DataFrame(result_arr, columns=self.props.get_value('resultCols'))

    def heuristics_based_on_spills(self, app_id_path: str) -> bool:
        """
        Apply heuristics based on spills to determine if the app can be accelerated on GPU.
        """
        # Load stage aggregation metrics (this contains spill information)
        job_stage_agg_metrics_file = self.props.get_value('spillBased', 'jobStageAggMetrics', 'fileName')
        job_stage_agg_metrics = pd.read_csv(os.path.join(app_id_path, job_stage_agg_metrics_file))
        job_stage_agg_metrics = job_stage_agg_metrics[self.props.get_value('spillBased',
                                                                           'jobStageAggMetrics', 'columns')]

        # Load sql-to-stage information (this contains Exec names)
        sql_to_stage_info_file = self.props.get_value('spillBased', 'sqlToStageInfo', 'fileName')
        sql_to_stage_info = pd.read_csv(os.path.join(app_id_path, sql_to_stage_info_file))
        sql_to_stage_info = sql_to_stage_info[self.props.get_value('spillBased',
                                                                   'sqlToStageInfo', 'columns')]

        # Identify stages with significant spills
        spill_threshold_bytes = self.props.get_value('spillBased', 'spillThresholdBytes')
        stages_with_spills = job_stage_agg_metrics[
            job_stage_agg_metrics['ID'].str.startswith('stage') &
            (job_stage_agg_metrics['diskBytesSpilled_sum'] +
             job_stage_agg_metrics['memoryBytesSpilled_sum'] > spill_threshold_bytes)
            ].copy()
        stages_with_spills['stageId'] = stages_with_spills['ID'].str.extract(r'(\d+)').astype(int)

        # Merge stages with spills with SQL-to-stage information
        merged_df = pd.merge(stages_with_spills, sql_to_stage_info, on='stageId', how='inner')

        # Identify stages with spills caused by Execs other than the ones allowed (Join, Aggregate or Sort)
        # Note: Column 'SQL Nodes(IDs)' contains the Exec names
        pattern = '|'.join(map(re.escape, self.props.get_value('spillBased', 'allowedExecs')))
        relevant_stages_with_spills = merged_df[~merged_df['SQL Nodes(IDs)'].apply(
            lambda x: isinstance(x, str) and bool(re.search(pattern, x)))]
        # If there are any stages with spills caused by non-allowed Execs, skip the application
        return len(relevant_stages_with_spills) > 0

    def apply_heuristics(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        try:
            additional_heuristics_df = self._apply_heuristics()
            all_apps = pd.merge(all_apps, additional_heuristics_df, on=['App Name', 'App ID'], how='left')
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error occurred while applying additional heuristics. '
                              'Reason - %s:%s', type(e).__name__, e)
        return all_apps
