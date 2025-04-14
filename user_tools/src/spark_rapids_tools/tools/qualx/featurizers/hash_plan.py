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

"""Default featurizer for Qualx."""

import hashlib
import json
import re
from typing import Optional

import pandas as pd

from spark_rapids_tools.tools.qualx.util import get_logger

logger = get_logger(__name__)


# expected features for the dataframe produced by preprocessing
# comments show the profiler source CSV file (and column name, if different)
# N/A indicates that the feature is derived from other features or other sources
expected_raw_features = {
    'hash',  # sql_plan_info_pre_aqe
}

# nodes to remove from the plan (exact match)
remove_nodes_exact = [
    # CPU
    'Sort',
]

# nodes to remove from the plan (prefix match)
remove_nodes_prefix = [
    # CPU
    'AdaptiveSparkPlan',
    'ColumnarToRow',
    'InputAdapter',
    'ReusedExchange',
    'WholeStageCodegen',
    # GPU
    'GpuCoalesceBatches',
    'GpuColumnarToRow',
    'GpuRapidsDeltaWrite',
    'GpuRowToColumnar',
    'GpuShuffleCoalesce',
    'GpuSort',
    'GpuInsertIntoHadoopFsRelationCommand',
    # Velox
    'RowToVeloxColumnar',
    'VeloxColumnarToRowExec',
    'VeloxRowToColumnar',
]

# nodes to rename in the plan
rename_nodes = {
    # strip suffixes
    'Scan parquet': 'Scan parquet',
    'Scan ExistingRDD Delta Table State': 'Scan ExistingRDD',
    'Scan ExistingRDD Delta Table Checkpoint': 'Scan parquet',
    # CPU
    'ObjectHashAggregate': 'HashAggregate',
    'ShuffledHashJoin': 'SortMergeJoin',
    'SortAggregate': 'HashAggregate',
    # GPU
    'Execute GpuInsertIntoHadoopFsRelationCommand': 'Execute InsertIntoHadoopFsRelationCommand',
    'Execute GpuInsertIntoHiveTable': 'Execute InsertIntoHiveTable',
    'GpuBroadcastExchange': 'BroadcastExchange',
    'GpuBroadcastHashJoin': 'BroadcastHashJoin',
    'GpuColumnarExchange': 'Exchange',
    'GpuCoalesce': 'Coalesce',
    'CpuCustomShuffleReader': 'AQEShuffleRead',
    'GpuExpand': 'Expand',
    'GpuExecute SaveIntoDataSourceCommand': 'Execute SaveIntoDataSourceCommand',
    'GpuFilter': 'Filter',
    'GpuGenerate': 'Generate',
    'GpuGlobalLimit': 'GlobalLimit',
    'GpuHashAggregate': 'HashAggregate',
    'GpuLocalLimit': 'LocalLimit',
    'GpuProject': 'Project',
    'GpuRunningWindow': 'Window',
    'GpuScan parquet': 'Scan parquet',
    'GpuShuffledHashJoin': 'SortMergeJoin',
    'GpuUnion': 'Union',
    # Velox
    'ProjectExecTransformer': 'Project',
}


def split_fields(s, *, normalize=False):
    """Split a string on commas, accounting for parentheses and CASE statements.

    Parameters
    ----------
    s: str
        String to split.
    normalize: bool
        If True, replace CASE WHEN..END statements with placeholder.

    Example:
    --------
    >>> split_fields('a, b(x), c(x,y), d(x,e(y)), CASE WHEN x INSET 1,2 THEN y ELSE z END', normalize=True)
    ['a', 'b(x)', 'c(x,y)', 'd(x,e(y))', 'case_statement']
    """
    if not s:
        return []

    if normalize:
        # replace CASE statements with a placeholder
        s = re.sub(r'CASE WHEN.*?END', 'case_statement', s, flags=re.DOTALL)

    # split on commas, accounting for nested parens
    field_start = 0
    depth = 0
    fields = []
    for i in range(len(s)):
        if s[i] == '(':
            depth += 1
        elif s[i] == ')':
            depth -= 1
        elif s[i] == ',' and depth == 0:
            fields.append(s[field_start:i].strip())
            field_start = i + 1
    fields.append(s[field_start:].strip())
    return fields


def strip_ids(s):
    """Remove trailing ids from a string.

    Example:
    --------
    >>> split_ids('foo#1234L'):
    foo
    """
    s = re.sub(r'#[0-9L]+', '', s)
    return s


def path_match(node, expected_path: list[str]):
    '''Check if a node matches a sub-path in a tree.'''
    if node['nodeName'] == expected_path[0]:
        if len(expected_path) == 1:
            return True
        elif len(node['children']) == 1:
            return path_match(node['children'][0], expected_path[1:])
        else:
            return False
    else:
        return False


def normalize_plan(plan):
    """Normalize the sparkPlanInfo for comparing CPU and GPU plans."""

    def normalize_node(node):
        """Normalize a single node in the plan by removing and/or renaming to canonical form."""
        if isinstance(node, dict):
            if 'nodeName' in node:
                if any([node['nodeName'] == name for name in remove_nodes_exact]) or any(
                    [node['nodeName'].startswith(name) for name in remove_nodes_prefix]
                ):
                    # remove nodes in the remove_nodes list
                    node['nodeName'] = rename_nodes.get(node['nodeName'], node['nodeName'])
                    if len(node['children']) == 1:
                        return normalize_node(node['children'][0])
                    else:
                        raise ValueError(f'Node {node["nodeName"]} should be removed, but has more than one child.')
                elif (
                    node['nodeName'] in ['Exchange', 'GpuColumnarExchange']
                    and 'REPARTITION_BY_NUM' in node['simpleString']
                ):
                    # special case: remove Exchange REPARTITION_BY_NUM
                    if len(node['children']) == 1:
                        return normalize_node(node['children'][0])
                    else:
                        raise ValueError(f'Node {node["nodeName"]} should be removed, but has more than one child.')
                else:
                    # rename nodes using the rename_nodes dictionary
                    for prefix in rename_nodes:
                        if node['nodeName'].startswith(prefix):
                            node['nodeName'] = rename_nodes[prefix]
                    node['nodeName'] = rename_nodes.get(node['nodeName'], node['nodeName'])
                    node['children'] = [normalize_node(child) for child in node['children']]
                    return node
            else:
                return node
        else:
            return node

    def normalize_path(node):
        """Normalize an entire sub-path in the plan.

        This requires normalize_node to have already been called.
        """
        if isinstance(node, dict):
            if 'nodeName' in node:
                if path_match(node, ['GpuTopN', 'Exchange', 'GpuTopN']):
                    # GpuTopN/Exchange/GpuTopN/X -> TakeOrderedAndProject/X
                    node['nodeName'] = 'TakeOrderedAndProject'
                    node['children'] = [
                        normalize_path(child) for child in node['children'][0]['children'][0]['children']
                    ]
                    return node
                elif path_match(node, ['Project', 'Filter']):
                    # Project/Filter/X -> Project/X
                    node['children'] = [normalize_path(child) for child in node['children'][0]['children']]
                    return node
                elif path_match(node, ['UnionWithLocalData', 'Union']):
                    # UnionWithLocalData/Union/X -> X
                    node = normalize_path(node['children'][0]['children'][0])
                    return node
                elif len(node['children']) == 1 and node['children'][0]['nodeName'] == 'SubqueryAdaptiveBroadcast':
                    # X/SubqueryAdaptiveBroadcast -> X
                    node['children'] = []
                    return node
                else:
                    # continue normalizing children
                    node['children'] = [normalize_path(child) for child in node['children']]
                    return node
            else:
                return node
        else:
            return node

    plan = normalize_node(plan)
    plan = normalize_path(plan)
    return plan


def hash_plan(plan):
    """Generate a unique hash for a physical plan.

    This will traverse each node in the plan tree and generate a hash for each node["nodeName"],
    which will be combined with the node's depth to generate a single hash for the plan tree.

    For Project nodes, the first output field from simpleString is included in the hash to
    distinguish between different projections.
    """

    def hash_node(node, depth=0):
        if isinstance(node, dict):
            node_name = node['nodeName']
            if node_name == 'Project':
                simple_string = node.get('simpleString', '')
                fields = re.search(r'\[(.*)\]', simple_string, flags=re.DOTALL)  # isolate content between '[' and ']'
                if fields:
                    fields = fields.group(1)
                    # remove field ids, e.g. #1234L
                    fields = strip_ids(fields)
                    # split fields by comma, accounting for function calls and CASE statements
                    fields = split_fields(fields, normalize=True)
                    # remove any fields with parentheses (functions) or brackets (maps) to simplify hashing
                    fields = [field.strip() for field in fields if not re.search(r"\(|\[", field)]
                    # take a max of 10 fields (to try to avoid truncated fields)
                    fields = fields[:10]
                else:
                    fields = ""

                node_str = f"{node_name}_{depth}_{fields}"
            else:
                node_str = f"{node_name}_{depth}"

            node_hash = int(hashlib.md5(node_str.encode()).hexdigest(), 16)
            return node_hash + sum([hash_node(child, depth + 1) for child in node['children']])
        else:
            return 0

    return hash_node(plan)


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
    # read all sql_plan_info_pre_aqe files
    full_tbl = []
    plan_files = toc.loc[toc['table_name'] == 'sql_plan_info_pre_aqe']
    for row in plan_files.itertuples():
        app_id = row.appId
        with open(row.filepath, 'r') as f:
            for json_line in f:
                json_obj = json.loads(json_line)
                sql_id = json_obj['sqlID']
                plan = json_obj['sparkPlanInfo']
                plan_hash = hash_plan(plan)
                # Note: store hash as a string to avoid long type limit in parquet
                full_tbl.append({'appId': app_id, 'sqlID': sql_id, 'hash': str(plan_hash)})
    df = pd.DataFrame(full_tbl)
    return df
