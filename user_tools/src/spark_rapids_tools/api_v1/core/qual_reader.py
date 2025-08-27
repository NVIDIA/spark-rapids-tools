# Copyright (c) 2025, NVIDIA CORPORATION.
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

"""Module that contains the definition of the Qualification core reader."""

from dataclasses import dataclass
from functools import cached_property

import pandas as pd

from spark_rapids_tools.api_v1.core.core_reader import CoreReaderBase
from spark_rapids_tools.api_v1.report_reader import register_report_class


@register_report_class('qualCoreOutput')
@dataclass
class QualCoreOutput(CoreReaderBase):
    """
    A class that reads the output of the Qual Core tool.
    """

    @cached_property
    def apps_summary_df(self) -> pd.DataFrame:
        result = self.load_df('qualCoreCSVSummary',
                              fall_cb=pd.DataFrame,
                              map_cols=None,
                              pd_args=None)
        # Drop duplicates based on App ID, keeping the one with highest Attempt ID
        df = result.data
        if not df.empty and 'App ID' in df.columns and 'Attempt ID' in df.columns:
            # Sort by Attempt ID in descending order, then drop duplicates on App ID keeping first (highest)
            df = df.sort_values('Attempt ID',
                                ascending=False).drop_duplicates(subset=['App ID'],
                                                                 keep='first')
        return df
