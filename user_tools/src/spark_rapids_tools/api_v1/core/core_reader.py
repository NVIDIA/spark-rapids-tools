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

"""Module that contains the definition of the base class that defines the common APIs between
Prof/Qual Readers."""

from dataclasses import dataclass
from functools import cached_property
from typing import Dict

import pandas as pd

from spark_rapids_tools import override
from spark_rapids_tools.api_v1 import AppHandler
from spark_rapids_tools.api_v1.report_reader import ToolReportReader
from spark_rapids_tools.utils.data_utils import LoadDFResult


@dataclass
class CoreReaderBase(ToolReportReader):
    """
    Base class for core report readers. It provides common functionality
    for reading core reports and loading application descriptions.
    Typically, a core reader has some more specific functionalities and capabilities. For example,
    1. the core-reader has the responsibility to list all the AppHandlers analyzed by the run.
    The reason we use a core-reader instead of a wrapper reader is that the core-reader has the
    actual list of eventlogs. So, it can be used to define all the eventlogPaths and categorize
    them into their perspective results.
    2. The core reader can define the version of the core-tools that generated the output results
    and whether it is compatible.
    """

    @property
    def _status_tbl(self) -> str:
        """Label for the CSV table in the report."""
        return 'coreCSVStatus'

    @cached_property
    def apps_status_res(self) -> LoadDFResult:
        """"
        Load the status table from the report. This table contains the status of each application
        processed by the core tools.
        :return: LoadDFResult containing the status data."""
        return self.load_df(self._status_tbl, fall_cb=pd.DataFrame)

    def _loads_apps_from_status(self) -> Dict[str, AppHandler]:
        """
        Load successful application descriptions from the status file.
        """
        res: Dict[str, AppHandler] = {}
        csv_res = self.apps_status_res
        if csv_res.success:
            for _, row in csv_res.data.iterrows():
                if row['Status'] == 'SUCCESS':
                    # filter out unsuccessful results.
                    app_entry = AppHandler(_app_id=row['App ID'],
                                           _attempt_id=row['Attempt ID'],
                                           _app_name=row['App Name'],
                                           eventlog_path=row['Event Log'])
                    res[app_entry.uuid] = app_entry
        return res

    @override
    def resolve_app_fds(self) -> None:
        """
        Loads the applications descriptors from the status CSV file.
        """
        self.app_fds.update(self._loads_apps_from_status())

    #################
    # Public APIs
    #################

    def analyzed_apps_count(self) -> int:
        """
        Returns the number of applications analyzed in the report. This is the total number of
        evetlogPaths that were visited by the core-tools. This will include failed/skipped
        applications as well.
        :return: The number of applications.
        """
        if not self.is_empty_result():
            return len(self.apps_status_res.data)
        return 0
