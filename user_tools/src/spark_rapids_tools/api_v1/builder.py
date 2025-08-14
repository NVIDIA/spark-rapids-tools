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
from logging import Logger
from typing import Union, Optional, TypeVar, Generic, List, Dict, Callable, Any, Set

import pandas as pd

from spark_rapids_tools import override
from spark_rapids_tools.api_v1 import ToolResultHandlerT, LoadCombinedRepResult, APIUtils
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

    ##########################
    # Public API for APIReport
    ##########################

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
        return self.rep_reader.is_per_app()

    @property
    def app_handlers(self) -> List[AppHandler]:
        """
        Return the list of application handlers associated with this report.

        If no specific apps are set, returns all app handlers from the handler.
        Otherwise, returns handlers for the specified apps.
        """
        if not self._apps:
            return list(self.handler.app_handlers.values())
        app_handlers = [
            self.handler.app_handlers.get(app_e) if isinstance(app_e, str) else app_e
            for app_e in self._apps
        ]
        return [app_h for app_h in app_handlers if app_h is not None]

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


@dataclass
class CSVReport(APIReport[LoadDFResult]):
    """
    A report that loads data in CSV format.

    This class extends APIReport to provide loading, mapping, and transformation of tabular data
    from CSV-based report tables. It supports column mapping, pandas read arguments, and fallback
    mechanisms for missing or malformed data.

    Attributes:
        _fall_cb (Optional[Callable[[], pd.DataFrame]]): A callback to provide a fallback DataFrame
            if loading fails or the data is missing.
        _map_cols (Optional[dict]): A static mapping from original column names to new column names
            to be applied when loading the DataFrame.
        _pd_args (Optional[dict]): Additional arguments to pass to pandas when reading the DataFrame,
            such as 'usecols', 'dtype', etc.
        _col_mapper_cb (Optional[Callable[[List[str]], Dict[str, str]]]): A callback that
            dynamically generates a column mapping based on the list of columns present in the table.

    Typical usage:
        - Configure the report with table label and application(s).
        - Optionally set column mapping, pandas arguments, or fallback callback.
        - Call load() to retrieve the data as a DataFrame or dictionary of DataFrames.

    See Also:
        APIReport, LoadDFResult
    """
    _fall_cb: Optional[Callable[[], pd.DataFrame]] = field(default=None, init=False)
    _map_cols: Optional[dict] = field(default=None, init=False)
    _pd_args: Optional[dict] = field(default=None, init=False)
    _col_mapper_cb: Optional[Callable[[List[str]], Dict[str, str]]] = field(default=None, init=False)

    def _check_map_col_args(self) -> None:
        """
        build the final column mapping of the report
        """
        temp_map_cols = {}
        if self._col_mapper_cb:
            # if the column mapper callback is defined, we need to build the static column.
            actual_tbl_cols = [col.name for col in self.rep_reader.get_table(self.tbl).columns]
            # check if the use_cols is defined
            if self._pd_args and 'usecols' in self._pd_args:
                # only pick those columns if defined
                selected_cols = self._pd_args['usecols']
            else:
                # if usecols is not defined, we will use all columns
                selected_cols = actual_tbl_cols
            # apply the map callback to the columns of the table
            temp_map_cols = self._col_mapper_cb(selected_cols)
        if self._map_cols is not None:
            # if the static map_cols is defined, then it is possible that we want to apply a
            # dynamic method to rename the columns, but for some reason there is a couple of fields
            # mapped statically.
            # overwrite the temp_map_cols keys with the static map_cols, or insert new ones.
            temp_map_cols.update(self._map_cols)
        if temp_map_cols:
            # finally re-assign the _map_cols field with the final column-mapping.
            self.map_cols(temp_map_cols)

    @override
    def _check_args(self) -> None:
        """Check if the required arguments are set."""
        super()._check_args()
        self._check_map_col_args()

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

    ##########################
    # Public API for CSVReport
    ##########################

    def map_cols_cb(self, cb: Callable[[List[str]], Dict[str, str]]) -> 'CSVReport':
        """Set the callback for mapping columns when loading data."""
        self._col_mapper_cb = cb
        return self

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

    def create_empty_df(self) -> pd.DataFrame:
        """
        Create an empty DataFrame with the columns defined in the report.
        :return: An empty DataFrame with the columns defined in the report.
        """
        tbl_df = self.handler.create_empty_df(self.tbl)
        # we need to handle if the columns were renamed, or selected
        if self._pd_args and 'usecols' in self._pd_args:
            # if usecols is defined, we need to filter the columns
            use_cols = self._pd_args['usecols']
            tbl_df = tbl_df[use_cols]
        if self._map_cols:
            tbl_df = tbl_df.rename(columns=self._map_cols)
        return tbl_df


@dataclass
class CSVReportCombiner(object):
    """
    A class for combining multiple CSVReport instances into a single DataFrame.

    This class is designed to aggregate per-application report data from multiple CSVReport objects,
    each potentially representing a different data source or configuration but sharing the same
    table schema. It manages the process of loading, combining, and post-processing data from these
    reports, handling both successful and failed application loads.

    Key Features:
        - Supports combining results from multiple CSVReport instances, each associated with a per-app table.
        - Handles injection of application metadata (such as app_id and other fields) into the resulting DataFrame.
        - Allows custom callbacks for processing successful and failed application loads.
        - Maintains records of failed and successful application loads for further inspection.
        - Can be configured to fall back to an empty DataFrame if all loads fail.
        - Provides utility methods for logging and argument validation.

    Typical Usage:
        1. Instantiate with a list of CSVReport objects.
        2. Optionally configure app field injection, success/failure callbacks, and fallback behavior.
        3. Call build() to produce a combined LoadDFResult containing the aggregated DataFrame and metadata about the
           load process.

    :param: rep_builders: List[CSVReport]. A list of CSVReport instances to combine.
    :param: _inject_app_ids_enabled: Bool, default True. Flag to enable/disable injection of
           application IDs into the combined DataFrame.
    :param: _app_fields: Optional[Dict[str, str]], default None. A dictionary that specifies the
           fields of the AppHandlers and how to inject them into the per-app DataFrame before they
           get combined.
           The expected structure is [field_name: str, column_name: str] where the normalized
           value of field_name is a valid field in AppHandler instance.
           Example of acceptable keys for AppHandler.app_id are: 'app_id', 'App ID', 'appId', 'app ID'.
    :param: _success_cb: Optional[Callable[[ToolResultHandlerT, str, pd.DataFrame], pd.DataFrame]],
           default None.
           A callback function that is called to provide any extra custom processing for each
           successful application. This cb is applied after injecting the app columns and right-before
           concatenating the app-dataframe to the combined dataFrame.
           This is useful in case the caller wants to apply some dataframe operations on the DataFrame.
           Note, it might be tricky if this cb is changing the shape of the DataFrame, as it might
           conflict with the expected Dataframe DTypes extracted from the original Table.
    :param: _failure_cb: Optional[Callable[[ToolResultHandlerT, str, LoadDFResult], Any]], default None.
           Provides custom handling for failed application loads. If the caller needs more processing
           than just appending the failures into the failed_apps dictionary, this callback can become
           handy.
    :raises:
        ValueError: If no report builders are provided or if argument validation fails.
        Exception: If an error occurs during the combination process and fallback is not enabled.

    See Also:
        CSVReport: For per-app report loading.
        AppHandler: For application metadata injection.
    """
    rep_builders: List[CSVReport]
    _inject_app_ids_enabled: bool = field(default=True, init=False)
    _app_fields: Optional[Dict[str, str]] = field(default=None, init=False)
    _success_cb: Optional[Callable[[ToolResultHandlerT, str, pd.DataFrame], pd.DataFrame]] = (
        field(default=None, init=False))
    _failure_cb: Optional[Callable[[ToolResultHandlerT, str, LoadDFResult], Any]] = (
        field(default=None, init=False))
    _fall_back_to_empty_df: bool = field(default=False, init=False)
    _failed_apps: Dict[str, Exception] = field(default_factory=dict, init=False)
    _successful_apps: Set[str] = field(default_factory=set, init=False)

    @property
    def failed_apps(self) -> Dict[str, Exception]:
        """Get the dictionary of failed applications."""
        return self._failed_apps

    @property
    def successful_apps(self) -> Set[str]:
        """Get the dictionary of failed applications."""
        return self._successful_apps

    @property
    def default_rep_builder(self) -> CSVReport:
        """Get the first report builder."""
        if not self.rep_builders:
            raise ValueError('No report builders provided for combination.')
        return self.rep_builders[0]

    @property
    def tbl(self) -> str:
        """Get the table label for the first report builder."""
        return self.default_rep_builder.tbl

    @property
    def app_handlers(self) -> List[AppHandler]:
        """
        Return the list of application handlers associated with the report builder.
        If no specific apps are set, returns all app handlers from the handler.
        Otherwise, returns handlers for the specified apps.

        Note that, it is possible to have duplicate apps if the same app is present in multiple reports.
        """
        res = []
        for rep_b in self.rep_builders:
            res.extend(rep_b.app_handlers)
        return res

    def disable_apps_injection(self) -> 'CSVReportCombiner':
        """Disable the injection of application IDs into the combined DataFrame."""
        self._inject_app_ids_enabled = False
        return self

    def on_app_fields(self, combine_on: Dict[str, str]) -> 'CSVReportCombiner':
        """Set the columns to combine on."""
        self._app_fields = combine_on
        return self

    def entry_success_cb(self,
                         cb: Callable[[ToolResultHandlerT, str, pd.DataFrame], pd.DataFrame]) -> 'CSVReportCombiner':
        """Set the callback for successful applications."""
        self._success_cb = cb
        return self

    def entry_failure_cb(self, cb: Callable[[ToolResultHandlerT, str, LoadDFResult], Any]) -> 'CSVReportCombiner':
        """Set the callback for failed applications"""
        self._failure_cb = cb
        return self

    def empty_df_on_error(self, fall_to_empty: bool = True) -> 'CSVReportCombiner':
        """Set whether to fall back to an empty DataFrame if no data is available."""
        self._fall_back_to_empty_df = fall_to_empty
        return self

    @staticmethod
    def logger(csv_rep: CSVReport) -> Logger:
        return csv_rep.handler.logger

    @staticmethod
    def log_failed_app(app_id: str, csv_rep: CSVReport, failed_load: LoadDFResult) -> None:
        """Log a failed application."""
        CSVReportCombiner.logger(csv_rep).debug(
            f'Failed to load {csv_rep.tbl} for app {app_id}: {failed_load.get_fail_cause()}')

    def _process_args(self) -> None:
        """Process the arguments to ensure they are set correctly."""
        if not self.rep_builders:
            raise ValueError('No report builders provided for combination.')
        if self._app_fields is None:
            # by default, we will use app_id column and it will be inserted as 'appId'
            # Later we can add more fields
            self.on_app_fields(AppHandler.get_default_key_columns())

    def _inject_app_into_df(
            self,
            res_h: ToolResultHandlerT,
            df: pd.DataFrame, app_id: str) -> pd.DataFrame:
        """
        Inject the application ID into the DataFrame.
        :param df: The DataFrame to inject the application ID into.
        :param app_id: The application ID to inject.
        :return: The DataFrame with the application ID injected.
        """
        # TODO: Should we check if app_obj is not found?
        app_obj = res_h.app_handlers.get(app_id)
        return app_obj.add_fields_to_dataframe(df, self._app_fields)

    def _build_single_report(self, csv_rep: CSVReport) -> List[pd.DataFrame]:
        combined_dfs = []
        # this is a dictionary and we should loop on it one by one to combine it
        per_app_res = csv_rep.load()
        for app_id, app_res in per_app_res.items():
            try:
                # Set a generic try-except block to handle unexpected errors for each entry to avoid
                # failing the entire combination process.
                if not app_res.success:  # what is the correct way to check for success?
                    # Process entry with failed results.
                    # 1. log debug message (We do not want error message because it will confuse the users)
                    # 2. Add it to the dictionary of failed apps
                    # 3. Call the failure callback if defined.
                    CSVReportCombiner.log_failed_app(app_id, csv_rep, app_res)
                    self._failed_apps[app_id] = app_res.get_fail_cause()
                    if self._failure_cb:
                        try:
                            self._failure_cb(csv_rep.handler, app_id, app_res)
                        except Exception as failure_cb_ex:  # pylint: disable=broad-except
                            # if the failure callback fails, we log it but do not raise an error.
                            CSVReportCombiner.logger(csv_rep).error(
                                f'Failed to apply  failure_cb for app {app_id} on {csv_rep.tbl}: {failure_cb_ex}')
                else:
                    # This is a successful result, we need to process it.
                    # 1. Append it to the list of successful apps.
                    # 2. Inject the app key columns into the dataframe if enabled.
                    # 3. Call the success callback if defined.
                    self._successful_apps.add(app_id)
                    processed_df = app_res.data
                    if self._inject_app_ids_enabled:
                        # inject the app_id into the dataframe
                        processed_df = self._inject_app_into_df(csv_rep.handler, app_res.data, app_id)
                    if self._success_cb:
                        # apply the success_callback defined by the caller
                        try:
                            processed_df = self._success_cb(csv_rep.handler, app_id, processed_df)
                        except Exception as success_cb_ex:  # pylint: disable=broad-except
                            # if the success callback fails, we log it but do not raise an error.
                            CSVReportCombiner.logger(csv_rep).error(
                                f'Failed to apply success_cb for app {app_id} on {csv_rep.tbl}: {success_cb_ex}')
                    # Q: Should we ignore or skip the empty dataframes?
                    combined_dfs.append(processed_df)
            except Exception as single_entry_ex:  # pylint: disable=broad-except
                # if any exception occurs during the processing of the app, we log it and continue.
                # add it to the failed_apps dictionary
                CSVReportCombiner.logger(csv_rep).error(
                    f'Failed to process app entry {app_id} for {csv_rep.tbl}: {single_entry_ex}')
                self._failed_apps[app_id] = single_entry_ex
        return combined_dfs

    def _create_empty_df(self) -> pd.DataFrame:
        """
        creates an empty DataFrame with the columns defined in the report builder.
        :return: an empty dataframe.
        """
        # get the dataframe based on user arguments.
        empty_df = self.default_rep_builder.create_empty_df()
        # if apps injection is enabled, we need to inject the app_id columns
        if self._inject_app_ids_enabled and self._app_fields:
            # make sure that we inject the app_id columns
            return AppHandler.inject_into_df(empty_df, self._app_fields)
        return empty_df

    def build(self) -> LoadDFResult:
        """Build the combined report."""
        # process teh arguments to ensure they are set correctly
        self._process_args()
        load_error = None
        final_df = None
        success = False
        fallen_back = False
        # loop on all the reports and combine their results
        try:
            combined_dfs = []
            for csv_rep in self.rep_builders:
                # load the report and combine the results.
                # if an exception is thrown in a single iteration, then their must be something completely wrong and we
                # need to fail.
                combined_dfs.extend(self._build_single_report(csv_rep))
            if combined_dfs:
                # only concatenate if we have any dataframes to combine
                final_df = pd.concat(combined_dfs, ignore_index=True)
            else:
                # create an empty DataFrame if no data was collected.
                final_df = self._create_empty_df()
            success = True
        except Exception as e:  # pylint: disable=broad-except
            # handle any exceptions that occur during combination phase or loading sub-reports
            load_error = e
            if self._fall_back_to_empty_df:
                # if we are falling back to an empty DataFrame, we create it here.
                try:
                    final_df = self._create_empty_df()
                    success = True
                    fallen_back = True
                except Exception as fall_back_ex:  # pylint: disable=broad-except
                    self.logger(self.default_rep_builder).error(
                        f'could not fall back to empty df {self.tbl}: {fall_back_ex}')
        return LoadDFResult(
            f_path='combination of multiple path for table: ' + self.tbl,
            data=final_df,
            success=success,
            fallen_back=fallen_back,
            load_error=load_error)


@dataclass
class APIHelpers(object):
    """
    A helper class for API v1 components.
    This class provides static methods to process results from CSVReportCombiner and handle exceptions.
    """
    @staticmethod
    def build_qual_core_handler(
            dir_path: Union[str, BoundedCspPath],
            raise_on_empty: bool = False) -> ToolResultHandlerT:
        """
        Builds and returns a ToolResultHandlerT for the Qual Core report.

        :param dir_path: The directory path or BoundedCspPath where the Qual Core report is located.
        :param raise_on_empty: If True, raises a ValueError if the report is empty or does not exist.
        :return: An instance of ToolResultHandlerT for the Qual Core report.
        :raises ValueError: If raise_on_empty is True and the report is empty or missing.
        """
        q_core_h = (
            APIResultHandler()
            .qual_core()
            .with_path(dir_path)
            .build()
        )
        if raise_on_empty and q_core_h.is_empty():
            raise ValueError(f'Qual Core report at {dir_path} is empty or does not exist.')
        return q_core_h

    @staticmethod
    def process_res(
            raw_res: LoadCombinedRepResult,
            combiner: CSVReportCombiner,
            convert_to_camel: bool = False,
            raise_on_failure: bool = True,
            raise_on_empty: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        A utility function that wraps the creation of a combined per-app-report.
        It processes the result of a CSVReportCombiner and handles exceptions.
        :param raw_res: The LoadRawFilesResult instance to append the results to.
        :param combiner: The CSVReportCombiner to process.
        :param convert_to_camel: If True, convert the column names to camelCase.
               This is useful to normalize the column-names to a common format.
        :param raise_on_failure: If True, raise an exception if the combiner fails to
               build the combined dataframe. Note that this is not intended to handle individual app-level
               failures. For the latter, visit the arguments provided by the CSVReportCombiner.
        :param raise_on_empty: If True, raise an exception if the resulting DataFrame is
               empty or None. This is useful to ensure that mandatory reports are always valid DataFrames
               while allow other optional reports to be empty without throwing exceptions.
        :return: The resulting DataFrame. The DataFrame can be None if there was an error while loading the report
               and the raise_on_empty is False. This cane be avoided if the combiner is configured to
               fall_back to an empty DataFrame in case of failure.
        :raises RuntimeError: If the combiner fails to build the final result and raise_on_failure is True.
        :raises ValueError: If the resulting DataFrame is empty and raise_on_empty is True.
        """
        try:
            if convert_to_camel:
                # Update the rep_builders by setting the col_map_cb to the staticmethod to_camel.
                for rep in combiner.rep_builders:
                    rep.map_cols_cb(APIUtils.cols_to_camel_case)
            # final_res is dictionary of [app_id: str, LoadDFResult] where each LoadDFResult
            # contains the data and the success flag.
            final_res = combiner.build()
            # If the combiner failed to load the report, we should raise an exception
            if not final_res.success and raise_on_failure:
                if final_res.load_error:
                    raise RuntimeError(
                        f'Loading report {combiner.tbl} failed with error: {final_res.get_fail_cause()}'
                    ) from final_res.get_fail_cause()  # use the get_fail_cause to get the original exception.
                raise RuntimeError(f'Loading report {combiner.tbl} failed with unexpected error.')
            # if dataframe is None, or dataframe is empty and raise_on_empty is True, raise an exception
            if raise_on_empty and (final_res.data is None or final_res.data.empty):
                detail_reason = 'a None DataFrame' if final_res.data is None else 'an empty DataFrame'
                raise ValueError(
                    f'Loading report {combiner.tbl} on dataset {raw_res.ds_name} returned {detail_reason}.')
            # If we reach this point, then there is no raise on exceptions, just proceed with wrapping up the report.
            # 1. Append failed apps: Loop on combiner_failed apps and append them to the raw_res.
            if combiner.failed_apps:
                for app_id, app_error in combiner.failed_apps.items():
                    raw_res.append_failure(app_id, combiner.tbl, app_error)
            # 2. Append the resulting dataframe to the reports if it is successful. Else set the Dataframe to None.
            if final_res.success:
                raw_res.append_success(combiner.tbl, final_res)
                return final_res.data
            # If the combiner failed to build the final result, we should None
            return None
        except Exception as e:  # pylint: disable=broad-except
            # If we reach here, it means that the combiner failed to build the final result.
            # We should raise an exception to inform the caller.
            raise RuntimeError(f'Failed to load report {combiner.tbl} on dataset {raw_res.ds_name}: {e}') from e
