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

"""Utilities for computing hashes of physical plans."""

from difflib import SequenceMatcher
import hashlib
import re
from typing import List, Optional

import pandas as pd

from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_tools.tools.qualx.hash_config import HashConfig


_hash_config = None


def get_hash_config() -> HashConfig:
    """Get the hash configuration."""
    global _hash_config  # pylint: disable=global-statement
    if _hash_config is None:
        _hash_config = HashConfig.load_from_file(str(Utils.resource_path('qualx-hash-conf.yaml')))
    return _hash_config


# Nodes to remove from the plan (using exact match):
# - that are only present in either CPU or GPU plans, e.g. GpuColumnarExchange.
# - that have simpleStrings that may be confused with other nodes, e.g. Sort (vs. SortMergeJoin).
remove_nodes_exact = get_hash_config().remove_nodes_exact

# Nodes to remove from the plan (using prefix match):
remove_nodes_prefix = get_hash_config().remove_nodes_prefix

# Nodes to rename (using prefix match):
# - with variable suffixes, e.g. Scan parquet LOCATION -> Scan parquet.
# - that are similar but have different names, e.g. BucketUnion -> Union.
rename_nodes = get_hash_config().rename_nodes


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
    for i, c in enumerate(s):
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        elif c == ',' and depth == 0:
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
    """Check if a node matches a sub-path in a tree.

    Parameters
    ----------
    node: dict
        The node to check.
    expected_path: list[str]
        The expected path (list of node names) to match.
    """
    if expected_path:
        if node['nodeName'] == expected_path[0]:
            if len(expected_path) == 1:
                return True
            return any(path_match(child, expected_path[1:]) for child in node['children'])
    return False


def normalize_plan(plan):
    """Normalize the sparkPlanInfo for comparing CPU and GPU plans.

    The high-level algorithm is a multi-step process:
    1. Traverse the plan tree to normalize individual nodes.
      - Remove any nodes in the remove_nodes_exact list
      - Remove any nodes by prefix match to the remove_nodes_prefix list
      - Rename nodes to a "canonical" form using the rename_nodes dictionary
      - Remove any SubqueryAdaptiveBroadcast children
    2. Traverse the plan tree to normalize entire paths (sequences of nodes).
      - GpuTopN/GpuTopN/X -> TakeOrderedAndProject/X
      - Project/Filter/X -> Project/X
      - UnionWithLocalData/Union/X -> X
    3. Traverse the plan tree to normalize BroadcastHashJoin subtrees.
    4. Traverse the plan tree to normalize SortMergeJoin/Project/BroadcastHashJoin subtrees.

    Notes:
    - The normalization should be applied to both CPU and GPU plans.
    - Eventlogs only contain physical plans, so use the earliest (least modified) physical plan.
    - The goal is to produce a "signature" of the plan and not a faithful re-creation of the logical plan.
    """

    def remove_nodes(node, remove_list) -> List[dict]:
        """Remove any nodes in the remove_list from a plan tree, while preserving children."""
        children = []
        for child in node['children']:
            children.extend(remove_nodes(child, remove_list))

        if node['nodeName'] in remove_list:
            return children

        node['children'] = children
        return [node]

    def normalize_node(node):
        """Normalize a single node in the plan by removing and/or renaming to canonical form.

        Notes:
        - SubqueryAdaptiveBroadcast children (and their subtrees) are also removed.
        """
        if isinstance(node, dict):
            if 'nodeName' in node:
                node_name = node['nodeName']
                # remove SubqueryAdaptiveBroadcast children
                node['children'] = [
                    child for child in node['children'] if child['nodeName'] != 'SubqueryAdaptiveBroadcast'
                ]
                if any(node_name == name for name in remove_nodes_exact):
                    # remove nodes in the remove_nodes_exact list
                    if len(node['children']) == 1:
                        return normalize_node(node['children'][0])
                    raise ValueError(f'Node {node_name} should be removed, but has more than one child.')
                if any(node_name.startswith(name) for name in remove_nodes_prefix):
                    # remove nodes in the remove_nodes_prefix list
                    if len(node['children']) == 1:
                        return normalize_node(node['children'][0])
                    raise ValueError(f'Node {node_name} should be removed, but has more than one child.')
                # otherwise, rename nodes using the rename_nodes dictionary
                for prefix, replacement in rename_nodes.items():
                    if node_name.startswith(prefix):
                        node['nodeName'] = replacement
                node['children'] = [normalize_node(child) for child in node['children']]
        return node

    def normalize_path(node):
        """Normalize an entire sub-path in the plan.

        This transforms a sub-path into a more canonical form, e.g.
        - GpuTopN/GpuTopN/X -> TakeOrderedAndProject/X
        - Project/Filter/X -> Project/X
        - UnionWithLocalData/Union/X -> Union/X

        This requires normalize_node to have already been called.
        """
        if isinstance(node, dict):
            if 'nodeName' in node:
                if path_match(node, ['GpuTopN', 'GpuTopN']):
                    # GpuTopN/GpuTopN/X -> TakeOrderedAndProject/X
                    node['nodeName'] = 'TakeOrderedAndProject'
                    node['children'] = [
                        normalize_path(child) for child in node['children'][0]['children']
                    ]
                elif path_match(node, ['Project', 'Filter']):
                    # Project/Filter/X -> Project/X
                    node['children'] = [normalize_path(child) for child in node['children'][0]['children']]
                elif path_match(node, ['UnionWithLocalData', 'Union']):
                    # UnionWithLocalData/Union/X -> X
                    node = normalize_path(node['children'][0]['children'][0])
                else:
                    # continue normalizing children
                    node['children'] = [normalize_path(child) for child in node['children']]
        return node

    def normalize_broadcast_hash_join(node):
        """Normalize a BroadcastHashJoin subtree."""
        if isinstance(node, dict):
            if node['nodeName'] == 'BroadcastHashJoin':
                # remove any Project/BroadcastHashJoin/BroadcastExchange/SortMergeJoin nodes
                remove_list = ['Project', 'BroadcastHashJoin', 'BroadcastExchange', 'SortMergeJoin']
                children = []
                for child in node['children']:
                    children.extend(remove_nodes(child, remove_list))
                node['children'] = children
            else:
                # continue normalizing children
                children = [normalize_broadcast_hash_join(child) for child in node['children']]
                node['children'] = children
        return node

    def normalize_sort_merge_join(node):
        """Normalize a SortMergeJoin/Project/BroadcastHashJoin subtree."""
        if isinstance(node, dict):
            if path_match(node, ['SortMergeJoin', 'Project', 'BroadcastHashJoin']):
                # remove Project/BroadcastHashJoin nodes
                remove_list = ['Project', 'BroadcastHashJoin']
                children = []
                for child in node['children']:
                    children.extend(remove_nodes(child, remove_list))
                node['children'] = children
                node['nodeName'] = 'BroadcastHashJoin'
            else:
                # continue normalizing children
                node['children'] = [normalize_sort_merge_join(child) for child in node['children']]
        return node

    normalized_plan = normalize_node(plan)
    normalized_plan = normalize_path(normalized_plan)
    normalized_plan = normalize_broadcast_hash_join(normalized_plan)
    normalized_plan = normalize_sort_merge_join(normalized_plan)

    return normalized_plan


def hash_plan(plan):
    """Generate a hash for a physical plan.

    The goal is to generate a "signature" of the plan that can be used to find similar plans.
    For comparison between CPU and GPU plans, normalize_plan should be called on both plans before hashing.

    The high-level algorithm is:
    - Traverse the plan tree to generate a hash for each node.
      - Each node's hash is a combination of the node's name and the node's depth.
        - For Project nodes, the first N fields from simpleString are also included in the hash, after:
          - removing any field/column ids, e.g. foo#1234L -> foo.
          - splitting fields by commas (accounting for expressions with multiple args).
          - ignoring any expression fields.
          - using a dummy string case_statement to represent any fields with CASE..END statements.
          - only taking a max of N (10) fields (to avoid issues with truncated simpleStrings).
      - Each node's hash is then combined with the hashes of its children.
    - Return the hash of the root node.
    """

    def hash_node(node, depth=0, n_fields=10):
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
                    fields = [field.strip() for field in fields if not re.search(r'\(|\[', field)]
                    # take a max of N fields (to try to avoid truncated fields)
                    fields = sorted(fields)
                    fields = fields[:n_fields]
                else:
                    fields = ''

                node_str = f'{node_name}_{depth}_{fields}'
            else:
                node_str = f'{node_name}_{depth}'

            node_hash = int(hashlib.md5(node_str.encode()).hexdigest(), 16)
            return node_hash + sum(hash_node(child, depth + 1, n_fields) for child in node['children'])
        return 0

    return hash_node(plan)


def get_junk_hashes(raw_features: pd.DataFrame, std_devs: float = 3.0) -> List[str]:
    """Identify junk hashes as high-occurrence outliers (beyond N standard deviations).

    Parameters
    ----------
    raw_features: pd.DataFrame
        Dataframe of raw features with a 'hash' column.
    std_devs: float
        Number of standard deviations to use for identifying junk hashes.
    """
    hash_counts = raw_features['hash'].value_counts()
    hash_counts_std = hash_counts.std()
    hash_counts_mean = hash_counts.mean()
    return list(hash_counts[hash_counts > hash_counts_mean + std_devs*hash_counts_std].index)


def max_intersection(df1: pd.DataFrame, df2: pd.DataFrame, junk_hashes: Optional[List[str]] = None) -> pd.DataFrame:
    """Find the maximum intersection between two dataframes of hashes.

    Parameters
    ----------
    df1: pd.DataFrame
        First dataframe of sqlIDs and hashes.
    df2: pd.DataFrame
        Second dataframe of sqlIDs and  hashes.
    junk_hashes: List[str]
        List of hashes which should be ignored when aligning, i.e. warm-up or metadata queries.

    Returns
    -------
    df: pd.DataFrame
        Dataframe of aligned SQL IDs.
    """
    # filter out junk hashes, if supplied
    if junk_hashes:
        df1 = df1.loc[~df1.hash.isin(junk_hashes)]
        df2 = df2.loc[~df2.hash.isin(junk_hashes)]

    list1, list2 = df1['hash'].tolist(), df2['hash'].tolist()

    seq_matcher = SequenceMatcher(None, list1, list2, autojunk=False)
    matches = seq_matcher.get_matching_blocks()

    # Extract the matching values forming LCS
    intersection_hashes = []
    intersection_ids1 = []
    intersection_ids2 = []

    for match in matches[:-1]:  # Exclude sentinel (0,0,0)
        hashes = list1[match.a: match.a + match.size]
        intersection_hashes.extend(hashes)
        # Retrieve the corresponding IDs from df1 and df2
        ids1 = df1.iloc[match.a: match.a + match.size]['sqlID'].tolist()
        ids2 = df2.iloc[match.b: match.b + match.size]['sqlID'].tolist()
        intersection_ids1.extend(ids1)
        intersection_ids2.extend(ids2)

    return pd.DataFrame(
        {
            'sqlID_cpu': intersection_ids1,
            'sqlID_gpu': intersection_ids2,
            'hash': intersection_hashes,
        }
    )


def align_sql_ids(
    cpu_hashes: pd.DataFrame,
    gpu_hashes: pd.DataFrame,
    junk_hashes: Optional[List[str]] = None
) -> pd.DataFrame:
    """Align CPU and GPU sqlIDs via plan hashes.

    Parameters
    ----------
    cpu_hashes: pd.DataFrame
        DataFrame of CPU sqlIDs and hashes.
    gpu_hashes: pd.DataFrame
        DataFrame of GPU sqlIDs and hashes.
    junk_hashes: List[str]
        List of hashes which should be ignored when aligning, i.e. warm-up or metadata queries.

    Returns
    -------
    df: pd.DataFrame
        DataFrame of aligned SQL IDs.
    """
    df = max_intersection(cpu_hashes, gpu_hashes, junk_hashes)
    df = df[['sqlID_cpu', 'sqlID_gpu', 'hash']]  # re-order columns
    return df
