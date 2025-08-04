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

"""Module that contains the entry for the API v1 builder."""

from dataclasses import dataclass, field
from typing import Union, Optional, TypeVar, Generic, List, Dict, Callable

import pandas as pd

from spark_rapids_tools import override
from spark_rapids_tools.api_v1 import ToolResultHandlerT
from spark_rapids_tools.api_v1 import AppHandler
from spark_rapids_tools.api_v1.report_loader import ReportLoader
from spark_rapids_tools.api_v1.report_reader import ToolReportReaderT
from spark_rapids_tools.storagelib.cspfs import BoundedCspPath
from spark_rapids_tools.utils.data_utils import LoadDFResult, JPropsResult, TXTResult


@dataclass
class APIResultHandler:
    """Builder for API v1 components."""
    _out_path: Optional[Union[str, BoundedCspPath]] = None
    _report_id: Optional[str] = None

    def report(self, rep_id: str) -> 'APIResultHandler':
        """Set the report ID for the API v1 components."""
        if not rep_id:
            raise ValueError('Report ID cannot be empty.')
        self._report_id = rep_id
        return self

    def with_path(self, out_arg: Union[str, BoundedCspPath]) -> 'APIResultHandler':
        """Set the output path for the API v1 components."""
        if not out_arg:
            raise ValueError('Output path cannot be empty.')
        self._out_path = out_arg
        return self

    def qual_core(self) -> 'APIResultHandler':
        """Set the report type to Qual Core."""
        self._report_id = 'qualCoreOutput'
        return self

    def prof_core(self) -> 'APIResultHandler':
        """Set the report type to Qual Core."""
        self._report_id = 'profCoreOutput'
        return self

    def qual_wrapper(self) -> 'APIResultHandler':
        """Set the report type to Qual Wrapper."""
        self._report_id = 'qualWrapperOutput'
        return self

    def build(self) -> ToolResultHandlerT:
        if self._report_id is None:
            raise ValueError('Report ID must be set before building.')
        if self._out_path is None:
            raise ValueError('Output path must be set before building.')
        repo_defn = ReportLoader()
        return repo_defn.create_result_handler(self._report_id, self._out_path)


RepDataT = TypeVar('RepDataT')


@dataclass
class APIReport(Generic[RepDataT]):
    """Base class for API reports that loads data from a report handler."""
    handler: ToolResultHandlerT
    _apps: Optional[List[Union[str, AppHandler]]] = field(default_factory=list, init=False)
    _tbl: Optional[str] = field(default=None, init=False)

    @property
    def rep_reader(self) -> 'ToolReportReaderT':
        """Get the report reader associated with this report."""
        if self._tbl is None:
            raise ValueError('Table must be set before accessing reader.')
        reader = self.handler.get_reader_by_tbl(self._tbl)
        if reader is None:
            raise ValueError(f'No reader found for table: {self._tbl}')
        return reader

    @property
    def tbl(self) -> str:
        """Get the table id."""
        return self._tbl

    @property
    def is_per_app_tbl(self) -> bool:
        if self._tbl is None:
            return False
        return self.rep_reader.is_per_app

    def _check_apps(self) -> None:
        """Check if applications are properly configured."""
        if not self.is_per_app_tbl:
            # this a global table, the apps list should not be defined
            if self._apps:
                # set the _apps from the handler itself
                raise ValueError('Applications cannot be specified for global tables.')

    def _check_tbl(self) -> None:
        """Check if the required arguments are set."""
        if self._tbl is None:
            raise ValueError('Table label must be set before loading data.')

    def _check_args(self) -> None:
        """Check if the required arguments are set."""
        self._check_tbl()
        self._check_apps()

    def _load_global(self) -> RepDataT:
        """Load the report data for a global table."""

    def _load_per_app(self) -> Dict[str, RepDataT]:
        """Load the report data for a per-application table."""

    def _load_single_app(self) -> RepDataT:
        """Load the report data for a single application."""

    def table(self, label: str):
        self._tbl = label
        return self

    def app(self, app: Union[str, AppHandler]):
        self._apps.append(app)
        return self

    def apps(self, apps: List[Union[str, AppHandler]]):
        self._apps.extend(apps)
        return self

    def load(self) -> Union[RepDataT, Dict[str, RepDataT]]:
        """Load the report data based on the specified table and applications."""
        self._check_args()

        # Load the data for the specified table and applications
        if not self.is_per_app_tbl:
            # this is a global table.
            return self._load_global()
        # this is a per-app table.
        if self._apps and len(self._apps) == 1:
            # this is single app selection, we need to return a single result
            return self._load_single_app()
        # this ia multiple-app selection, we need to return a dictionary
        return self._load_per_app()


@dataclass
class CSVReport(APIReport[LoadDFResult]):
    """A report that loads data in CSV format."""
    _fall_cb: Optional[Callable[[], pd.DataFrame]] = field(default=None, init=False)
    _map_cols: Optional[dict] = field(default=None, init=False)
    _pd_args: Optional[dict] = field(default=None, init=False)

    def fall_cb(self, cb: Callable[[], pd.DataFrame]) -> 'CSVReport':
        """Set the fallback callback for loading data."""
        self._fall_cb = cb
        return self

    def map_cols(self, cols: dict) -> 'CSVReport':
        """Set the column mapping for loading data."""
        self._map_cols = cols
        return self

    def pd_args(self, args: dict) -> 'CSVReport':
        """Set the pandas arguments for loading data."""
        self._pd_args = args
        return self

    @override
    def _load_global(self) -> LoadDFResult:
        return self.rep_reader.load_df(
            self._tbl,
            fall_cb=self._fall_cb,
            map_cols=self._map_cols,
            pd_args=self._pd_args
        )

    @override
    def _load_per_app(self) -> Dict[str, LoadDFResult]:
        return self.rep_reader.load_apps_df(
            self._tbl,
            apps=self._apps,
            fall_cb=self._fall_cb,
            map_cols=self._map_cols,
            pd_args=self._pd_args
        )

    @override
    def _load_single_app(self) -> LoadDFResult:
        # this is single app selection, we need to return a single result
        return self.rep_reader.load_app_df(
            self._tbl,
            app=self._apps[0],
            fall_cb=self._fall_cb,
            map_cols=self._map_cols,
            pd_args=self._pd_args
        )


@dataclass
class CSVCombiner(object):
    """A class that combines multiple CSV reports into a single report."""
    rep_builder: CSVReport
    _failed_app_processor: Optional[Callable[[str, LoadDFResult], None]] = field(default=None, init=False)
    _success_app_processor: Optional[Callable[[ToolResultHandlerT, str, pd.DataFrame, dict], pd.DataFrame]] = (
        field(default=None, init=False))
    _combine_args: Optional[dict] = field(default=None, init=False)

    @property
    def result_handler(self) -> ToolResultHandlerT:
        """Get the result handler associated with this combiner."""
        return self.rep_builder.handler

    @staticmethod
    def default_success_app_processor(result_handler: ToolResultHandlerT,
                                      app_id: str,
                                      df: pd.DataFrame,
                                      combine_args: dict) -> pd.DataFrame:
        """Default processor for successful applications."""
        col_names = None
        app_entry = result_handler.app_handlers.get(app_id)
        if not app_entry:
            raise ValueError(f'App entry not found for ID: {app_id}')
        if combine_args:
            # check if the col_names are provided to stitch the app_ids
            col_names = combine_args.get('col_names', None)
        if col_names:
            # patch the app_uuid and if the columns are defined.
            return app_entry.patch_into_df(df, col_names=col_names)
        return df

    def _evaluate_args(self) -> None:
        """Evaluate the arguments to ensure they are set correctly."""

        if self._success_app_processor is None:
            # set the default processor for successful applications
            self._success_app_processor = self.default_success_app_processor
        # TODO: we should fail if the the combiner is built for AppIds but columns are not defined.

    ################################
    # Setters/Getters for processors
    ################################

    def process_failed(self,
                       processor: Callable[[str, LoadDFResult], None]) -> 'CSVCombiner':
        """Set the processor for failed applications."""
        self._failed_app_processor = processor
        return self

    def process_success(self,
                        cb_fn: Callable[[ToolResultHandlerT, str, pd.DataFrame, dict], pd.DataFrame]) -> 'CSVCombiner':
        """Set the processor for successful applications."""
        self._success_app_processor = cb_fn
        return self

    def combine_args(self, args: dict) -> 'CSVCombiner':
        """Set the arguments for combining the reports."""
        self._combine_args = args
        return self

    def on_apps(self) -> 'CSVCombiner':
        """specify that the combiner should append IDs to the individual results before the concatenation."""
        self.process_success(self.default_success_app_processor)
        return self

    #########################
    # Public Interfaces
    #########################

    def build(self) -> LoadDFResult:
        """Build the combined CSV report."""
        # process teh arguments to ensure they are set correctly
        self._evaluate_args()

        load_error = None
        final_df = None
        success = False
        try:
            per_app_res = self.rep_builder.load()
            # this is a dictionary and we should loop on it one by one to combine it
            combined_dfs = []
            for app_id, app_res in per_app_res.items():
                # we need to patch the app_id to the dataframe
                if app_res.load_error or app_res.data.empty:
                    # process entry with failed results or skip them if no handlder is defined.
                    if self._failed_app_processor:
                        self._failed_app_processor(app_id, app_res)
                    else:
                        # default behavior is to skip the app
                        continue
                else:
                    # process entry with successful results
                    app_df = self._success_app_processor(self.result_handler,
                                                         app_id,
                                                         app_res.data,
                                                         self._combine_args)
                    # Q: Should we ignore or skip the empty dataframes?
                    combined_dfs.append(app_df)
            final_df = pd.concat(combined_dfs, ignore_index=True)
            success = True
        except Exception as e:  # pylint: disable=broad-except
            # handle any exceptions that occur during the combination phase
            load_error = e
        return LoadDFResult(
            f_path='combination of multiple path for table: ' + self.rep_builder.tbl,
            data=final_df,
            success=success,
            fallen_back=False,
            load_error=load_error)


@dataclass
class JPropsReport(APIReport[JPropsResult]):
    """A report that loads data in JSON properties format."""

    @override
    def _load_global(self) -> JPropsResult:
        # this is a global table.
        return self.rep_reader.load_jprop(self._tbl)

    @override
    def _load_per_app(self) -> Dict[str, JPropsResult]:
        return self.rep_reader.load_apps_jprop(
            self._tbl,
            apps=self._apps)

    @override
    def _load_single_app(self) -> JPropsResult:
        return self.rep_reader.load_app_jprop(
            self._tbl,
            app=self._apps[0])


@dataclass
class TXTReport(APIReport[TXTResult]):
    """A report that loads data in TXT format."""
    @override
    def _load_global(self) -> TXTResult:
        # this is a global table.
        return self.rep_reader.load_txt(self._tbl)

    @override
    def _load_per_app(self) -> Dict[str, TXTResult]:
        return self.rep_reader.load_apps_txt(
            self._tbl,
            apps=self._apps)

    @override
    def _load_single_app(self) -> TXTResult:
        return self.rep_reader.load_app_txt(
            self._tbl,
            app=self._apps[0])
