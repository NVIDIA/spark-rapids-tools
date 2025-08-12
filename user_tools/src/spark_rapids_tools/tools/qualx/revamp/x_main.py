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

""" Main module for QualX related commands """

from pathlib import Path
from typing import Optional, List

import pandas as pd

from spark_rapids_tools.api_v1 import QualCoreResultHandler
from spark_rapids_tools.tools.qualx.config import get_config
from spark_rapids_tools.tools.qualx.revamp.preprocess import load_profiles
from spark_rapids_tools.tools.qualx.util import get_logger

logger = get_logger(__name__)


def predict_x(
        platform: str,
        qual: str,
        output_info: dict,
        *,
        model: Optional[str] = None,
        qual_tool_filter: Optional[str] = None,
        config: Optional[str] = None,
        qual_handlers: List[QualCoreResultHandler]
) -> pd.DataFrame:
    """
    sddsdssd
    :param platform:
    :param qual:
    :param output_info:
    :param model:
    :param qual_tool_filter:
    :param config:
    :param qual_handlers:
    :return:
    """
    # load config from command line argument, or use default
    cfg = get_config(config)
    model_type = cfg.model_type
    model_config = cfg.__dict__.get(model_type, {})
    qual_filter = qual_tool_filter if qual_tool_filter else model_config.get('qual_tool_filter', 'stage')

    if not qual_handlers:
        raise ValueError('qual_handlers list is empty - no qualification data available for prediction')
    if all(q_handler.is_empty() for q_handler in qual_handlers):
        logger.warning('All qualification handlers are empty - no apps to predict')
        return pd.DataFrame()
        # if qualification metrics are provided, load metrics and apply filtering
    datasets = {}                       # create a dummy dataset
    dataset_name = Path(qual).name      # use qual directory name as dataset name
    datasets[dataset_name] = {
        'resultHandlers': qual_handlers,
        'app_meta': {'default': {'runType': 'CPU', 'scaleFactor': 1}},
        'platform': platform,
    }
    logger.debug('Loading dataset: %s', dataset_name)
    # load the raw files from the qualification handlers
    load_profiles(datasets=datasets, qual_tool_filter=qual_filter, remove_failed_sql=False)

    return pd.DataFrame()
