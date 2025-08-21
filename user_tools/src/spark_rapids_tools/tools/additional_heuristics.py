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

"""Implementation class for Additional Heuristics logic."""

import os
import re
from dataclasses import dataclass, field
from logging import Logger

import pandas as pd

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.tools.qualx.util import find_paths, RegexPattern
from spark_rapids_tools.utils import Utilities


@dataclass
class AdditionalHeuristics:
    """
    Encapsulates the logic to apply additional heuristics to skip applications.
    """
    logger: Logger = field(default=None, init=False)
    props: JSONPropertiesContainer = field(default=None, init=False)
    tools_output_dir: str = field(default=None, init=False)
    output_file: str = field(default=None, init=False)
    # Contains apps info needed for applying heuristics
    all_apps: pd.DataFrame = field(default=None, init=False)

    def __init__(self, props: dict, tools_output_dir: str, output_file: str):
        self.props = JSONPropertiesContainer(props, file_load=False)
        self.tools_output_dir = tools_output_dir
        self.output_file = output_file
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.{self.__class__.__name__}')

    def _get_all_heuristics_functions(self) -> list:
        """
        Returns a list of heuristics functions to apply to each application.
        """
        return [self.heuristics_based_on_spills]

    def _apply_heuristics(self, app_ids: list) -> pd.DataFrame:
        """
        Apply additional heuristics to applications to determine if they can be accelerated on GPU.
        """
        qual_metrics = find_paths(
            self.tools_output_dir,
            RegexPattern.qual_tool_metrics.match,
            return_directories=True,
        )
        if len(qual_metrics) == 0:
            self.logger.warning('No metrics found in output directory: %s', self.tools_output_dir)
            return pd.DataFrame(columns=self.props.get_value('resultCols'))

        if len(qual_metrics) > 1:
            # We don't expect multiple metrics directories. Log a warning and use the first one.
            self.logger.warning('Unexpected multiple metrics directories found. Using the first one: %s',
                                qual_metrics[0])

        metrics_path = qual_metrics[0]
        result_arr = []
        if not os.listdir(metrics_path) or len(app_ids) == 0:
            self.logger.warning('Skipping empty metrics folder: %s', qual_metrics[0])
        else:
            for app_id in app_ids:
                app_id_path = os.path.join(metrics_path, app_id)
                # Apply a list of heuristics and determine if the application should be skipped.
                # List of valid heuristics is loaded from the qualification-conf.yaml file.
                should_skip_overall = False
                reasons = []
                for heuristic_func in self._get_all_heuristics_functions():
                    try:
                        should_skip, reason = heuristic_func(app_id_path)
                    except Exception as e:  # pylint: disable=broad-except
                        should_skip = False
                        reason = f' Cannot apply heuristics for qualification. Reason - {type(e).__name__}:{e}.'
                        self.logger.error(reason)
                    should_skip_overall = should_skip_overall or should_skip
                    reasons.append(reason)
                reasons_text = ' '.join(reasons) if reasons and any(reasons) else None
                result_arr.append([app_id, should_skip_overall, reasons_text])

        return pd.DataFrame(result_arr, columns=self.props.get_value('resultCols'))

    def heuristics_based_on_spills(self, app_id_path: str) -> (bool, str):
        """
        Apply heuristics based on spills to determine if the app can be accelerated on GPU.
        """
        # Load stage aggregation metrics (this contains spill information)
        stage_agg_metrics_file = self.props.get_value('spillBased', 'stageAggMetrics', 'fileName')
        stage_agg_metrics = pd.read_csv(os.path.join(app_id_path, stage_agg_metrics_file))
        stage_agg_metrics = stage_agg_metrics[self.props.get_value('spillBased',
                                                                   'stageAggMetrics', 'columns')]

        # Load sql-to-stage information (this contains Exec names)
        sql_to_stage_info_file = self.props.get_value('spillBased', 'sqlToStageInfo', 'fileName')
        sql_to_stage_info = pd.read_csv(os.path.join(app_id_path, sql_to_stage_info_file))
        sql_to_stage_info = sql_to_stage_info[self.props.get_value('spillBased',
                                                                   'sqlToStageInfo', 'columns')]

        # Identify stages with significant spills
        # Convert the string to int because the parse_config method returns a string
        spill_threshold_bytes = int(self.props.get_value('spillBased', 'spillThresholdBytes'))
        spill_condition = stage_agg_metrics['memoryBytesSpilled_sum'] > spill_threshold_bytes
        stages_with_spills = stage_agg_metrics[spill_condition]

        # Merge stages with spills with SQL-to-stage information
        merged_df = pd.merge(stages_with_spills, sql_to_stage_info, on='stageId', how='inner')

        # Identify stages with spills caused by Execs other than the ones allowed (Join, Aggregate or Sort)
        # Note: Column 'SQL Nodes(IDs)' contains the Exec names
        pattern = '|'.join(map(re.escape, self.props.get_value('spillBased', 'allowedExecs')))
        relevant_stages_with_spills = merged_df[~merged_df['SQL Nodes(IDs)'].apply(
            lambda x: isinstance(x, str) and bool(re.search(pattern, x)))]
        # If there are any stages with spills caused by non-allowed Execs, skip the application
        if not relevant_stages_with_spills.empty:
            spill_threshold_human_readable = Utilities.bytes_to_human_readable(spill_threshold_bytes)
            reason = f'Skipping due to total data spill in stages exceeding {spill_threshold_human_readable}'
            return True, reason
        return False, ''

    def apply_heuristics(self, all_apps: pd.DataFrame) -> pd.DataFrame:
        try:
            self.all_apps = all_apps
            heuristics_df = self._apply_heuristics(all_apps['App ID'].unique())
            # Save the heuristics results to a file and drop the reason column
            heuristics_df.to_csv(self.output_file, index=False)
            all_apps = pd.merge(all_apps, heuristics_df, on=['App ID'], how='left')
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error('Error occurred while applying additional heuristics. '
                              'Reason - %s:%s', type(e).__name__, e)
        return all_apps
