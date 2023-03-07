# Copyright (c) 2022, NVIDIA CORPORATION.
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
"""Data validation tool basic program."""

import json
import logging
import re
import sys
from typing import Callable

import fire
import pkg_resources

from spark_rapids_dataproc_tools.utilities import get_log_dict, run_cmd

# Setup logging
logger = logging.getLogger('diag')

consoleHandler = logging.StreamHandler(sys.stdout)
logFormatter = logging.Formatter('%(message)s')
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)


class Diagnostic:
    """Diagnostic tool basic class."""

    def __init__(self, debug=False):
        if debug:
            logging.config.dictConfig(get_log_dict({'debug': debug}))
            logger.setLevel(logging.DEBUG)

        # Diagnostic summary
        self.summary = {}
        self.nv_mvn_repo = 'https://repo1.maven.org/maven2/com/nvidia'