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

"""Align CPU and GPU sqlIDs using provided alignment or inferred alignment from plan hash values."""

from difflib import SequenceMatcher
from typing import List, Optional

import numpy as np
import pandas as pd

from spark_rapids_tools.tools.qualx.qualx_config import QualxConfig
from spark_rapids_tools.tools.qualx.util import get_logger

logger = get_logger(__name__)


def get_junk_hashes(raw_features: pd.DataFrame, std_devs: float = 3.0) -> List[str]:
    """Identify junk hashes as high-occurrence outliers (beyond N standard deviations)."""
    hash_counts = raw_features['hash'].value_counts()
    hash_counts_std = hash_counts.std()
    hash_counts_mean = hash_counts.mean()
    return list(hash_counts[hash_counts > hash_counts_mean + std_devs*hash_counts_std].index)


def max_intersection(df1, df2, junk_hashes: Optional[List[str]] = None):
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
        hashes = list1[match.a : match.a + match.size]
        intersection_hashes.extend(hashes)
        # Retrieve the corresponding IDs from df1 and df2
        ids1 = df1.iloc[match.a : match.a + match.size]['sqlID'].tolist()
        ids2 = df2.iloc[match.b : match.b + match.size]['sqlID'].tolist()
        intersection_ids1.extend(ids1)
        intersection_ids2.extend(ids2)

    return pd.DataFrame(
        {
            'sqlID_cpu': intersection_ids1,
            'sqlID_gpu': intersection_ids2,
            'hash': intersection_hashes,
        }
    )


def align_sql_ids(cpu_hashes: pd.DataFrame, gpu_hashes: pd.DataFrame, junk_hashes: Optional[List[str]] = None):
    """Align CPU and GPU sqlIDs via physical plan hashes.

    This will generate a `sqlID_align.csv` file in the `output_dir`.

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


def compute_alignment_from_raw_features(raw_features: pd.DataFrame) -> pd.DataFrame:
    """Align CPU and GPU sqlIDs based on raw_features only.

    Brute force search for best GPU appId match for each CPU appId, based on the number of matching hash values.

    Returns
    -------
    alignment_df: pd.DataFrame
        Alignment dataFrame with columns ['appId_cpu', 'appId_gpu', 'sqlID_cpu', 'sqlID_gpu', 'hash'].
    """
    junk_hashes = get_junk_hashes(raw_features)

    alignment_dfs = []
    for app_name in raw_features['appName'].unique():
        app_features = raw_features.loc[raw_features['appName'] == app_name]

        cpu_hashes = app_features.loc[app_features['runType'] == 'CPU'][['appId', 'sqlID', 'hash']]
        cpu_hashes = cpu_hashes.sort_values(['appId', 'sqlID'])

        gpu_hashes = app_features.loc[app_features['runType'] == 'GPU'][['appId', 'sqlID', 'hash']]
        gpu_hashes = gpu_hashes.sort_values(['appId', 'sqlID'])

        cpu_app_ids = cpu_hashes['appId'].unique()
        gpu_app_ids = gpu_hashes['appId'].unique()

        # compute best gpu_app_id matches for each cpu_app_id
        alignments = {}  # {cpu_app_id: {gpu_app_id: hash_matches}}
        for cpu_app_id in cpu_app_ids:
            cpu_hash_df = cpu_hashes.loc[cpu_hashes['appId'] == cpu_app_id]
            best_matches = {}  # {gpu_app_id: hash_matches}
            best_match_len = 0
            for gpu_app_id in gpu_app_ids:
                gpu_hash_df = gpu_hashes.loc[gpu_hashes['appId'] == gpu_app_id]
                align_df = align_sql_ids(cpu_hash_df, gpu_hash_df, junk_hashes=junk_hashes)
                num_matches = len(align_df)
                if num_matches == 0:
                    continue
                if num_matches > best_match_len:
                    # if there is a new best match, replace the old one
                    best_matches = {gpu_app_id: align_df}
                    best_match_len = num_matches
                elif num_matches == best_match_len:
                    # if there are multiple matches, keep all of them
                    best_matches.update({gpu_app_id: align_df})

            if best_matches and best_match_len > 0:
                alignments[cpu_app_id] = best_matches

        # resolve multiple alignments
        matched_gpu_app_ids = set()
        # for cpu_app_ids with more than one best match, keep the first match for each gpu_app_id
        for cpu_app_id, best_matches in alignments.items():
            # filter out gpu_app_ids that have already been matched
            best_matches = {
                gpu_app_id: align_df for gpu_app_id, align_df in best_matches.items()
                if gpu_app_id not in matched_gpu_app_ids
            }
            if len(best_matches) > 0:
                # keep the first match for each gpu_app_id in sorted order
                gpu_app_ids = sorted(best_matches.keys())
                gpu_app_id = gpu_app_ids[0]
                align_df = best_matches[gpu_app_id]
                matched_gpu_app_ids.add(gpu_app_id)
                align_df['appId_cpu'] = cpu_app_id
                align_df['appId_gpu'] = gpu_app_id
                alignment_dfs.append(align_df)

    if alignment_dfs:
        alignment_df = pd.concat(alignment_dfs)
    else:
        alignment_df = pd.DataFrame(columns=['appId_cpu', 'appId_gpu', 'sqlID_cpu', 'sqlID_gpu', 'hash'])

    return alignment_df


def compute_alignment_from_app_pairs(raw_features: pd.DataFrame, alignment_df: pd.DataFrame) -> pd.DataFrame:
    """Compute CPU and GPU sqlID alignments based on aligned CPU and GPU appId pairs."""
    junk_hashes = get_junk_hashes(raw_features)

    new_align_df = pd.DataFrame(columns=['appId_cpu', 'appId_gpu', 'sqlID_cpu', 'sqlID_gpu', 'hash'])
    for row in alignment_df.itertuples():
        app_id_cpu, app_id_gpu = row.appId_cpu, row.appId_gpu
        cpu_hash_df = raw_features.loc[raw_features['appId'] == app_id_cpu][['sqlID', 'hash']]
        gpu_hash_df = raw_features.loc[raw_features['appId'] == app_id_gpu][['sqlID', 'hash']]
        align_df = align_sql_ids(cpu_hash_df, gpu_hash_df, junk_hashes=junk_hashes)
        if align_df.empty:
            # if empty, add a dummy row, so we can identify/debug missing alignments
            align_df = pd.DataFrame({'sqlID_cpu': -1, 'sqlID_gpu': -1, 'hash': np.nan}, index=[0])
        align_df['appId_cpu'] = app_id_cpu
        align_df['appId_gpu'] = app_id_gpu
        new_align_df = pd.concat([new_align_df, align_df])

    return new_align_df


def modify(
        raw_features: pd.DataFrame,
        config: QualxConfig,
        alignment_df: pd.DataFrame) -> pd.DataFrame:
    """Align CPU and GPU sqlIDs based on alignment_df.

    If alignment_df is not provided or doesn't contain sqlID alignments, infer sqlID alignments using plan hash values.
    """
    # pylint: disable=unused-argument
    if alignment_df is None or alignment_df.empty:
        logger.info('Computing appId and sqlID alignment from plan hashes')
        alignment_df = compute_alignment_from_raw_features(raw_features)
    elif 'sqlID_cpu' not in alignment_df.columns or 'sqlID_gpu' not in alignment_df.columns:
        logger.info('Computing sqlID alignment from plan hashes and provided appId pairs')
        alignment_df = compute_alignment_from_app_pairs(raw_features, alignment_df)
    else:
        logger.info('Using provided appId and sqlID alignment')

    if alignment_df.empty:
        return raw_features

    cpu_rows = raw_features.loc[raw_features['runType'] == 'CPU'].copy()
    gpu_rows = raw_features.loc[raw_features['runType'] == 'GPU'].copy()

    # Use appId_cpu as description to uniquely identify each pair of CPU and GPU runs
    cpu_rows['description'] = cpu_rows['appId']

    # update GPU sqlIDs to match CPU sqlIDs using align_df
    gpu_index = gpu_rows.index
    gpu_rows = gpu_rows.merge(
        alignment_df,
        how='left',
        left_on=['appId', 'sqlID'],
        right_on=['appId_gpu', 'sqlID_gpu'])
    gpu_rows.index = gpu_index

    gpu_rows.rename(
        columns={
            'sqlID': 'sqlID_orig',
            'sqlID_cpu': 'sqlID',
            'description': 'description_orig',
            'appId_cpu': 'description'
        },
        inplace=True
    )

    # fill na so we can update the original dataframe
    gpu_rows['sqlID'] = gpu_rows['sqlID'].astype(float)
    gpu_rows = gpu_rows.fillna(
        value={'sqlID': -1, 'description': 'gpu_do_not_match'}
    )

    # update the original dataframe
    raw_features.update(cpu_rows)
    raw_features.update(gpu_rows)

    return raw_features
