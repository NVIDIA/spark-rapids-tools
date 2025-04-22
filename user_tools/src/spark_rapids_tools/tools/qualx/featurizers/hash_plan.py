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

"""Featurizer for hashing physical plans."""

import json
from typing import Optional

import pandas as pd

from spark_rapids_tools.tools.qualx.util import get_logger
from spark_rapids_tools.tools.qualx.hash_util import hash_plan, normalize_plan

logger = get_logger(__name__)


# expected features for the dataframe produced by preprocessing
# comments show the profiler source file (and column name, if different)
# N/A indicates that the feature is derived from other features or other sources
expected_raw_features = {
    'hash',  # sql_plan_info_pre_aqe
}


def extract_raw_features(
    toc: pd.DataFrame,
    node_level_supp: Optional[pd.DataFrame],
    qualtool_filter: Optional[str],
    qualtool_output: Optional[pd.DataFrame] = None,
    remove_failed_sql: bool = True,
) -> pd.DataFrame:
    """Given a pandas dataframe of CSV files, extract raw features into a single dataframe keyed by (appId, sqlID).

    Parameters
    ----------
    toc: pd.DataFrame
        Table of contents of CSV files for the dataset.
    node_level_supp: pd.DataFrame
        Node-level support information used to filter out metrics associated with unsupported operators.
    qualtool_filter: str
        Type of filter to apply to the qualification tool output, either 'stage' or None.
    qualtool_output: pd.DataFrame
        Qualification tool output.
    remove_failed_sql: bool
        Remove sqlIDs with high failure rates, default: True.
    """
    # pylint: disable=unused-argument

    # read all sql_plan_info_pre_aqe files
    full_tbl = []
    plan_files = toc.loc[toc['table_name'] == 'sql_plan_info_pre_aqe']
    for row in plan_files.itertuples():
        app_id = row.appId
        with open(row.filepath, 'r', encoding='utf-8') as f:
            for json_line in f:
                json_obj = json.loads(json_line)
                sql_id = json_obj['sqlID']
                plan = json_obj['sparkPlanInfo']
                normalized_plan = normalize_plan(plan)
                plan_hash = hash_plan(normalized_plan)
                # Note: store hash as a string to avoid long type limit in parquet
                full_tbl.append({'appId': app_id, 'sqlID': sql_id, 'hash': str(plan_hash)})

    if full_tbl:
        df = pd.DataFrame(full_tbl)
    else:
        df = pd.DataFrame(columns=['appId', 'sqlID', 'hash'])

    return df
