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

"""Utility functions for preprocessing for QualX"""

from typing import Mapping, Optional

import pandas as pd


from spark_rapids_tools.tools.qualx.config import get_config
from spark_rapids_tools.tools.qualx.preprocess import infer_app_meta, get_alignment
from spark_rapids_tools.tools.qualx.revamp.featurizer import extract_raw_features


def load_profiles(
        datasets: Mapping[str, Mapping],
        *,
        qual_tool_filter: Optional[str] = None,
        remove_failed_sql: bool = True) -> pd.DataFrame:
    """
    Load and preprocess the profiles from the datasets.
    :param datasets: A mapping of dataset names to their profiles.
    :param qual_tool_filter: Optional filter for the qualification tool output, either 'stage' or None.
    :param remove_failed_sql: If True, remove profiles with failed SQL.
    :raises KeyError: If dataSet does not have 'app_meta' or 'eventlogs' defined.
    :raises ValueError: If the default app_meta is not defined or if it has multiple entries.
    :return: A DataFrame containing the loaded profiles.
    """
    # define a dict to load all the plugins
    plugins = {}
    config = get_config()
    alignment_df = get_alignment()
    # the line below is modified to enforce using the revamp methods
    # featurizers: List[Callable[[str, List[ToolResultHandlerT]], pd.DataFrame]] = [extract_raw_features]
    for ds_name, ds_meta in datasets.items():
        # get platform from dataset metadata, or use onprem if not provided
        platform = ds_meta.get('platform', 'onprem')
        if 'load_profiles_hook' in ds_meta:
            plugins[ds_name] = ds_meta['load_profiles_hook']
        if 'eventlogs' in ds_meta:
            # dataset has an entry for eventlogs, this implies that we need to get the app_meta,
            # or infer from directory structure of eventlogs.
            app_meta = ds_meta.get('app_meta', infer_app_meta(ds_meta['eventlogs']))
        else:
            # dataset does not have an entry for eventlogs, so we can use the app_meta directly.
            # This will raise a keyError if app_meta is not defined.
            app_meta = ds_meta['app_meta']
        # get the array of resultHandlers from the dataset metadata.
        core_res_handlers = ds_meta['resultHandlers']

        # get default app_meta
        app_meta_default = app_meta['default']
        if len(app_meta) != 1:
            raise ValueError(f'Default app_meta for {ds_name} cannot be used with additional entries.')
        app_meta_default = {'runType': 'CPU', 'scaleFactor': 1}

        # TODO: Should we merge the result handlers or just keep it that way?
        # invoke featurizers to extract raw features from profiler output
        features = extract_raw_features(
            ds_name=ds_name,
            res_hs=core_res_handlers,
            qualtool_filter=qual_tool_filter,
            remove_failed_sql=remove_failed_sql
        )
        if features is not None:
            # add the features to the dataset metadata
            return features

    return pd.DataFrame()
