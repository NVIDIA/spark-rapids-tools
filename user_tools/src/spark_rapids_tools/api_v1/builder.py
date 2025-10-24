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
from functools import partial, cached_property
from logging import Logger
from typing import Union, Optional, TypeVar, Generic, List, Dict, Callable, Any, Set, ClassVar

import pandas as pd

from spark_rapids_tools import override
from spark_rapids_tools.api_v1 import AppHandler, ProfCoreResultHandler
from spark_rapids_tools.api_v1 import (
    ToolReportReaderT,
    ToolResultHandlerT,
    LoadCombinedRepResult,
    ProfWrapperResultHandler,
    QualCoreResultHandler,
    QualWrapperResultHandler,
    result_registry
)
from spark_rapids_tools.api_v1.report_loader import ReportLoader
from spark_rapids_tools.storagelib.cspfs import BoundedCspPath
from spark_rapids_tools.utils.data_utils import LoadDFResult, JPropsResult, TXTResult, JSONResult

RepDataT = TypeVar('RepDataT')


@dataclass
class APIReport(Generic[RepDataT]):
    """
    Base class for API reports that loads data from a report handler.

    This class provides a generic interface for loading, validating, and accessing report data
    from a specified handler. It supports both global and per-application tables, context management
    for loading and cleanup, and utility methods for argument checking and data retrieval.

    Attributes:
        handler (ToolResultHandlerT): The result handler used to load report data.
        _tbl (Optional[str]): The label of the table to load.
        _apps (Optional[List[Union[str, AppHandler]]]): List of application IDs or handlers to filter data.
        _load_res (LoadResult): The result of the load operation stored as part of the object.
                                It can be either dictionary of RepDataT or a single RepDataT

    Methods:
        table(label): Set the table label to load.
        app(app): Add a single application to filter data.
        apps(apps): Add multiple applications to filter data.
        load(): Load the report data based on the specified table and applications.
        __enter__(): Context manager entry to load data.
        __exit__(): Context manager exit for cleanup and error handling.
    """
    handler: ToolResultHandlerT
    _tbl: Optional[str] = field(default=None)
    _apps: Optional[List[Union[str, AppHandler]]] = field(default_factory=list)
    _load_res: Optional[Union[RepDataT, Dict[str, RepDataT]]] = field(default=None, init=False)

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

    def _check_handler(self) -> None:
        """Check if the handler is properly configured."""
        if self.handler is None:
            raise ValueError('Handler must be set before loading data.')

    def _check_args(self) -> None:
        """Check if the required arguments are set."""
        self._check_handler()
        self._check_tbl()
        self._check_apps()

    def _load_global(self) -> RepDataT:
        """Load the report data for a global table."""

    def _load_per_app(self) -> Dict[str, RepDataT]:
        """Load the report data for a per-application table."""

    def _load_single_app(self) -> RepDataT:
        """Load the report data for a single application."""

    def __enter__(self) -> RepDataT:
        """
        Context manager entry point to load the data.
        :return: LoadDFResult or Dict[str, LoadDFResult] depending on the table type.
        """
        self._load_res = self.load()
        return self._load_res

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point to handle cleanup and error propagation.
        It returns False to propagate any error that occurred within the context. If no such
        error occurred, it checks the load result for success and raises an exception if the
        load failed and no fallback was provided.
        """
        # if there is no exception within the context, then we check whether the load was successful.
        if exc_val is None and self.load_res is not None:
            # if the load is unsuccessful and there is no exception, then we can throw an error
            if not isinstance(self.load_res, dict):
                # We only do this in case of a single loadResult object. Otherwise, per-app loads
                # return a dictionary and each entry should be handled by the caller case-by-case.
                if not self.load_res.success:
                    # this means that there was failure and the fall-back did not work
                    raise RuntimeError(f'Failed to load {self.tbl}') from self.load_res.get_fail_cause()
        # propagate any exception that occurred within the context.
        return False

    ##########################
    # Public API for APIReport
    ##########################

    @property
    def load_res(self) -> Optional[Union[RepDataT, Dict[str, RepDataT]]]:
        """Get the result of the last load operation."""
        return self._load_res

    @cached_property
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
class JSONReport(APIReport[JSONResult]):
    """A report that loads data in JSON format."""
    @override
    def _load_global(self) -> JSONResult:
        # this is a global table.
        return self.rep_reader.load_json(self._tbl)

    @override
    def _load_per_app(self) -> Dict[str, JSONResult]:
        return self.rep_reader.load_apps_json(
            self._tbl,
            apps=self._apps)

    @override
    def _load_single_app(self) -> JSONResult:
        return self.rep_reader.load_app_json(
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
            Compared to static mapping (_map_cols), this allows for more flexible handling of
            column names that may change over time or across different reports.
        _fall_back_to_empty_df (bool): If True, and there is an exception, the report will fall back
            to returning an empty DataFrame
        _load_res: The result of the last load operation stored as part of the object.
    Typical usage:
        - Configure the report with table label and application(s).
        - Optionally set column mapping, pandas arguments, or fallback callback.
        - Call load() to retrieve the data as a DataFrame or dictionary of DataFrames.

    See Also:
        APIReport, LoadDFResult
    """
    _fall_cb: Optional[Callable[[], pd.DataFrame]] = field(default=None)
    _map_cols: Optional[dict] = field(default=None)
    _pd_args: Optional[dict] = field(default=None)
    _col_mapper_cb: Optional[Callable[[List[str]], Dict[str, str]]] = field(default=None)
    _fall_back_to_empty_df: bool = field(default=False)

    def _check_fall_back_args(self) -> None:
        """
        Check if the fallback callback is set and valid.
        If not set, it will default to returning an empty DataFrame.
        """
        if self._fall_cb is None:
            # if the fall-back callback is not defined and fall_to_empty is enabled
            # then we will set the fall-back callback to return an empty DataFrame with datatypes
            # defined.
            if self._fall_back_to_empty_df:
                self._fall_cb = self.create_empty_df

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
        self._check_fall_back_args()

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

    This class is designed to aggregate report data from multiple CSVReport objects,
    each potentially representing a different data source or configuration but sharing the same
    table schema. It manages the process of loading, combining, and post-processing data from these
    reports, handling both successful and failed application loads.
    It can be used for both types of reports:
        - per-application (i.e., files that are listed in the per-app folders such as app_information); and
        - global (i.e., files that are not listed in the per-app folders such as qual_summary).

    Key Features:
        - Supports combining results from multiple CSVReport instances, each associated with a per-app table.
        - Handles injection of application metadata (such as app_id and other fields) into the resulting DataFrame.
        - Allows custom callbacks for processing successful and failed application loads.
        - Maintains records of failed and successful entry loads for further inspection.
        - Can be configured to fall back to an empty DataFrame if all loads fail.
        - Provides utility methods for logging and argument validation.

    Typical Usage:
        1. Instantiate with a list of CSVReport objects.
        2. Optionally configure app field injection, success/failure callbacks, and fallback behavior.
        3. Call build() to produce a combined LoadDFResult containing the aggregated DataFrame and metadata about the
           load process.

    :param rep_builders: List[CSVReport]. A list of CSVReport instances to combine.
    :param _inject_app_ids_enabled: Bool, default True. Flag to enable/disable injection of
           application IDs into the combined DataFrame.
    :param _app_fields: Optional[Dict[str, str]], default None. A dictionary that specifies the
           fields of the AppHandlers and how to inject them into the per-app DataFrame before they
           get combined.
           The expected structure is [field_name: str, column_name: str] where the normalized
           value of field_name is a valid field in AppHandler instance.
           Example of acceptable keys for AppHandler.app_id are: 'app_id', 'App ID', 'appId', 'app ID'.
    :param _success_cb: Optional[Callable[[ToolResultHandlerT, str, pd.DataFrame], pd.DataFrame]],
           default None.
           A callback function that is called to provide any extra custom processing for each
           successful application. This cb is applied after injecting the app columns and right-before
           concatenating the app-dataframe to the combined dataFrame.
           This is useful in case the caller wants to apply some dataframe operations on the DataFrame.
           Note, it might be tricky if this cb is changing the shape of the DataFrame, as it might
           conflict with the expected Dataframe DTypes extracted from the original Table.
    :param _failure_cb: Optional[Callable[[ToolResultHandlerT, str, LoadDFResult], Any]], default None.
           Provides custom handling for failed application loads. If the caller needs more processing
           than just appending the failures into the failed_apps dictionary, this callback can become
           handy.
    :param _fall_back_to_empty_df: bool, default False. If True, the combiner will return an empty
           DataFrame if there is an error during the combination process. When False, an excpetion
           will be captured by the LoadDFResult object, and a NONE data value.
    :param _failed_loads: Dict[str, Exception], default {}. A dictionary that maps IDs to the exception
           raised during the loading of each item. In case of per-app report, this is a map between
           app_id and the exception. For global reports, it is the folder name and the exception.
    :param _successful_loads: Set[str], default {}. A set of IDs that were successfully loaded.
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
    _failed_loads: Dict[str, Exception] = field(default_factory=dict, init=False)
    _successful_loads: Set[str] = field(default_factory=set, init=False)

    @staticmethod
    def logger(csv_rep: CSVReport) -> Logger:
        return csv_rep.handler.logger

    @staticmethod
    def log_failed_entry(entry_id: str, csv_rep: CSVReport, failed_load: LoadDFResult) -> None:
        """Log a failed entry."""
        CSVReportCombiner.logger(csv_rep).info(
            f'Failed to load [{csv_rep.tbl}] for entry [{entry_id}]: {failed_load.get_fail_cause()}')

    def _process_args(self) -> None:
        """Process the arguments to ensure they are set correctly."""
        if not self.rep_builders:
            raise ValueError('No report builders provided for combination.')
        # Check if the default report is of type global, then the injection must be disabled.
        if not self.default_rep_builder.is_per_app_tbl:
            self.disable_apps_injection()
        if self._app_fields is None:
            # by default, we will use app_id column and it will be inserted as 'appId'
            # Later we can add more fields
            self.on_app_fields(AppHandler.get_default_key_columns())

    def _inject_app_into_df(
            self,
            res_h: ToolResultHandlerT,
            df: pd.DataFrame,
            app_id: str
    ) -> pd.DataFrame:
        """
        Inject the application ID into the DataFrame.
        :param df: The DataFrame to inject the application ID into.
        :param app_id: The application ID to inject.
        :return: The DataFrame with the application ID injected.
        """
        # TODO: Should we check if app_obj is not found?
        app_obj = res_h.app_handlers.get(app_id)
        return app_obj.add_fields_to_dataframe(df, self._app_fields)

    def _process_res_entry(
            self,
            csv_rep: CSVReport,
            entry_id: str,
            entry_res: LoadDFResult,
            combined_dfs: List[pd.DataFrame]
    ) -> None:
        """
        Process a single result entry for a given app or global report.

        This method handles both successful and failed LoadDFResult entries:
        - On failure: logs the failure, records the exception in _failed_loads, and calls the failure
          callback if provided.
        - On success: injects app metadata if enabled, applies the success callback if provided, and
          appends the processed DataFrame to the combined_dfs list.

        :param csv_rep: The CSVReport instance being processed.
        :param entry_id: The ID of the entry to process. e.g., app_id in case of per-app report or
               folder name in case of global-report.
        :param entry_res: The LoadDFResult object containing the result for this entry.
        :param combined_dfs: The list to which the processed DataFrame will be appended if successful.
        :return: None. The processed DataFrame is appended to combined_dfs on success.
        """
        try:
            # Set a generic try-except block to handle unexpected errors for each entry to avoid
            # failing the entire combination process.
            if not entry_res.success:  # what is the correct way to check for success?
                # Process entry with failed results.
                # 1. log debug message (We do not want error message because it will confuse the users)
                # 2. Add it to the dictionary of failed apps
                # 3. Call the failure callback if defined.
                CSVReportCombiner.log_failed_entry(entry_id, csv_rep, entry_res)
                self._failed_loads[entry_id] = entry_res.get_fail_cause()
                if self._failure_cb:
                    try:
                        self._failure_cb(csv_rep.handler, entry_id, entry_res)
                    except Exception as failure_cb_ex:  # pylint: disable=broad-except
                        # if the failure callback fails, we log it but do not raise an error.
                        CSVReportCombiner.logger(csv_rep).error(
                            f'Failed to apply failure_cb for app {entry_id} on {csv_rep.tbl}: {failure_cb_ex}')
            else:
                # This is a successful result, we need to process it.
                # 1. Append it to the list of successful apps.
                # 2. Inject the app key columns into the dataframe if enabled.
                # 3. Call the success callback if defined.
                self._successful_loads.add(entry_id)
                processed_df = entry_res.data
                if self._inject_app_ids_enabled:
                    # inject the app_id into the dataframe
                    processed_df = self._inject_app_into_df(csv_rep.handler, entry_res.data, entry_id)
                if self._success_cb:
                    # apply the success_callback defined by the caller
                    try:
                        processed_df = self._success_cb(csv_rep.handler, entry_id, processed_df)
                    except Exception as success_cb_ex:  # pylint: disable=broad-except
                        # if the success callback fails, we log it but do not raise an error.
                        CSVReportCombiner.logger(csv_rep).error(
                            f'Failed to apply success_cb for entry [{entry_id}] on {csv_rep.tbl}: {success_cb_ex}')
                # Q: Should we ignore or skip the empty dataframes?
                combined_dfs.append(processed_df)
        except Exception as single_entry_ex:  # pylint: disable=broad-except
            # if any exception occurs during the processing of a single entry, we log it and continue.
            # add it to the failed_loads dictionary
            CSVReportCombiner.logger(csv_rep).error(
                f'Failed to process entry [{entry_id}] for {csv_rep.tbl}: {single_entry_ex}')
            self._failed_loads[entry_id] = single_entry_ex

    def _build_global_combined_report(self, csv_rep: CSVReport) -> List[pd.DataFrame]:
        """
        Builds a combined global report for the given `CSVReport` instance.

        :param csv_rep: The `CSVReport` instance to process and combine results from.
        :return: A list of `pd.DataFrame` objects, each representing the processed data for the global report.
        """
        combined_dfs: List[pd.DataFrame] = []
        # this is a single LoadDFResult
        single_global_res = csv_rep.load()
        # process the entry
        self._process_res_entry(
            csv_rep,
            entry_id=csv_rep.handler.get_folder_name(),
            entry_res=single_global_res,
            combined_dfs=combined_dfs
        )
        return combined_dfs

    def _build_per_app_combined_report(self, csv_rep: CSVReport) -> List[pd.DataFrame]:
        """
        Builds a combined per-app report for the given `CSVReport` instance.

        :param csv_rep: The `CSVReport` instance to process and combine results from.
        :return: A list of `pd.DataFrame` objects, each representing the processed data for an application.
        """
        combined_dfs: List[pd.DataFrame] = []
        # this is a dictionary and we should loop on it one by one to combine it
        per_app_res = csv_rep.load()
        for app_id, app_res in per_app_res.items():
            # process each entry in the per-app result
            self._process_res_entry(csv_rep, entry_id=app_id, entry_res=app_res, combined_dfs=combined_dfs)
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

    ###################################
    # Public API for Combined CSVReport
    ###################################

    @property
    def failed_loads(self) -> Dict[str, Exception]:
        """
        Get the dictionary of failed loads.
        In the case of per-app report, it is a map between app_ids and the exception."""
        return self._failed_loads

    @property
    def successful_ids(self) -> Set[str]:
        """Get the Ids with successful loads."""
        return self._successful_loads

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
            processor_cb = partial(self._build_per_app_combined_report)
            if not self.default_rep_builder.is_per_app_tbl:
                # if the default report is a global table, we need to use the global processor.
                processor_cb = partial(self._build_global_combined_report)
            for csv_rep in self.rep_builders:
                # Load the report and combine the results.
                # if an exception is thrown in a single iteration, then their must be something completely wrong and we
                # need to fail.
                combined_dfs.extend(processor_cb(csv_rep))
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


class CombinedCSVBuilderMeta(type):
    """
    Metaclass for CombinedCSVBuilder to create instances with flexible handler input.
    This metaclass allows the CombinedCSVBuilder to accept either a single ToolResultHandlerT
    or a list of APIResHandler[ToolResultHandlerT] instances as the handlers argument.
    It processes the input to extract the actual handlers and passes them to the CombinedCSVBuilder
    constructor.
    This design simplifies the instantiation of CombinedCSVBuilder by allowing users to provide
    handlers in different formats without needing to manually extract the handler objects.
    For example, users can pass a list of APIResHandler objects directly, and the metaclass will
    handle the extraction of the underlying ToolResultHandlerT instances. This shields the users' code
    from dealing with the internal structure of APIResHandler.
    On the other hand, internally, initializations that need to create the CombinedCSVBuilder on a
    single handler can benefit from passing the handler directly without wrapping it in a list.
    """
    def __call__(
            cls,
            table: str,
            handlers: Union[ToolResultHandlerT, List['APIResHandler[ToolResultHandlerT]']],
            *args,
            **kwargs
    ) -> 'CombinedCSVBuilder':
        if isinstance(handlers, list):
            handlers_arg = [r.handler for r in handlers]
        else:
            handlers_arg = handlers
        instance: 'CombinedCSVBuilder' = super(CombinedCSVBuilderMeta, cls).__call__(
            table=table, handlers=handlers_arg, *args, **kwargs)
        return instance


@dataclass
class CombinedCSVBuilder(metaclass=CombinedCSVBuilderMeta):
    """
    A builder class for creating and combining multiple CSVReport instances into a single DataFrame.

    This builder simplifies the process of aggregating tabular report data from one or more result
    handlers (e.g., qualification or profiling reports), configuring report options, and producing
    a combined DataFrame with robust error handling.

    Typical usage involves:
        - Instantiating the builder with a table name and one or more ToolResultHandlerT objects
          (handlers).
        - Optionally configuring error handling, callbacks, or additional report options.
        - Using the context manager to create and configure CSVReport objects for each handler.
        - Calling build() to produce the combined DataFrame.

    Parameters:
        table (str): The table label to load from each handler.
        handlers (ToolResultHandlerT or List[ToolResultHandlerT]): One or more result handlers to aggregate.
        raise_on_failure (bool, optional): Raise an exception if the combination fails. Default: True.
        raise_on_empty (bool, optional): Raise an exception if the resulting DataFrame is empty. Default: False.
        res_container (LoadCombinedRepResult, optional): Optional container to collect success/failure results.

    Example usages:
        # Example 1: Combine qualification reports from multiple handlers
        from spark_rapids_tools.api_v1.builder import APIHelpers
        handlers = [qual_handler1, qual_handler2]
        with CombinedCSVBuilder(
            table='app_information',
            handlers=handlers
        ) as builder:
            # Optionally configure each CSVReport
            def configure_csv_report(rep):
                return rep.pd_args({'usecols': ['col1', 'col2']})
            builder.apply_on_report(configure_csv_report)
            combined_df = builder.build()

        # Example 2: Combine qualification reports from multiple handlers and configure
          each report using inline lambda functions.
        from spark_rapids_tools.api_v1.builder import APIHelpers
        handlers = [qual_handler1, qual_handler2]
        with CombinedCSVBuilder(
            table='app_information',
            handlers=handlers
        ) as builder:
            # Optionally pick specific columns from each report and rename columns
            builder.apply_on_report(
                lambda x: x.pd_args(
                    {'usecols': ['col1', 'col2']}
                ).map_cols({'col1': 'new_col1'})
            )
            combined_df = builder.build()

        # Example 3: Combine a report and specify combination arguments
        with CombinedCSVBuilder(
            table='my_table',
            handlers=qual_handler
        ) as builder:
            builder.raise_on_empty = True
            # 1. use an empty DataFrame if the report fails to be combined
            # 2. specify appID to be the injected column and app_name into the DataFrame
            builder.combiner.empty_df_on_error(
                fall_to_empty=True
            ).on_app_fields({'app_id': 'appID'})
            combined_df = builder.build()

        # Example 4: Use with a result container for tracking failures
        res_container = LoadCombinedRepResult(res_id='qual_run_001')
        with CombinedCSVBuilder(
            table='app_information',
            handlers=handlers,
            res_container=res_container
        ) as builder:
            combined_df = builder.build()
            # res_container now contains details of failed/successful loads

    See Also:
        CSVReport, CSVReportCombiner, ToolResultHandlerT, LoadCombinedRepResult
    """
    table: str
    handlers: Union[ToolResultHandlerT, List[ToolResultHandlerT]]
    raise_on_failure: Optional[bool] = field(default=True)
    raise_on_empty: Optional[bool] = field(default=False)
    res_container: Optional[LoadCombinedRepResult] = field(default=None)
    _csv_reports: List[CSVReport] = field(default_factory=list, init=False)
    _combiner: Optional[CSVReportCombiner] = field(default=None, init=False)

    @cached_property
    def res_handlers(self) -> List[ToolResultHandlerT]:
        """
        Get the list of result handlers.
        :return: A list of ToolResultHandlerT instances.
        """
        if isinstance(self.handlers, list):
            return self.handlers
        return [self.handlers]

    def _init_reports(self) -> None:
        """
        Initialize the CSVReport instances for each result handler.
        :return: None. The CSVReport instances are stored in the _csv_reports attribute.
        """
        if not self.res_handlers:
            raise ValueError('No result handlers provided for CSVReportCombiner.')
        # create a CSVReport for each result handler
        for res_h in self.res_handlers:
            self._csv_reports.append(
                CSVReport(res_h, _tbl=self.table)
            )

    def __enter__(self) -> 'CombinedCSVBuilder':
        """
        Context manager entry point to start building the CSVReportCombiner.
        It initializes the CSVReports for each result handler.
        :return: The builder instance for chaining.
        """
        self._init_reports()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Propagate the exception if any occurred during the context.
        """
        return False

    ###################################
    # Public API for API Helpers
    ###################################

    def suppress_failure(self) -> 'CombinedCSVBuilder':
        """Set raise_on_failure to False."""
        self.raise_on_failure = False
        return self

    def fail_on_empty(self) -> 'CombinedCSVBuilder':
        """Set raise_on_empty to True."""
        self.raise_on_empty = True
        return self

    def with_container_result(self, container: LoadCombinedRepResult) -> 'CombinedCSVBuilder':
        """Set the result container."""
        self.res_container = container
        return self

    def apply_on_report(self, csv_rep_cb: Callable[[CSVReport], CSVReport]) -> 'CombinedCSVBuilder':
        """
        Apply a callBack on the CSVReport to configure it.
        This is useful to apply any custom configuration on each CSVReport before building the combiner.
        Note that the callback is applied on each CSVReport instance created for each result handler.
        :param csv_rep_cb: A callback that takes a CSVReport and returns a configured CSVReport.
                           Example of inlined callback:
                           lambda rep: rep.pd_args({'usecols': ['col1', 'col2']})
        :return: The builder instance for chaining.
        """
        if not self.res_handlers:
            raise ValueError('No result handlers provided for CSVReportCombiner.')
        for rep in self._csv_reports:
            csv_rep_cb(rep)
        return self

    @property
    def combiner(self) -> CSVReportCombiner:
        """
        Build and return the CSVReportCombiner instance.
        This is lazy initializer because the combiner construction need to be deferred until
        the CSVReports are fully initialized.
        :return: The constructed CSVReportCombiner.
        """
        if self._combiner is None:
            self._combiner = CSVReportCombiner(self._csv_reports)
        return self._combiner

    def build(self) -> Optional[pd.DataFrame]:
        """
        Build and return the CSVReportCombiner instance.
        :return: The constructed combined Dataframe.
        :raises RuntimeError: If the combiner fails to build the final result and
                raise_on_failure is True.
        """
        try:
            # final_res is dictionary of [app_id: str, LoadDFResult] where each LoadDFResult
            # contains the data and the success flag.
            final_res = self.combiner.build()
            # If the combiner failed to load the report, we should raise an exception
            if not final_res.success and self.raise_on_failure:
                if final_res.load_error:
                    raise RuntimeError(
                        f'Loading report {self.table} failed with error: {final_res.get_fail_cause()}'
                    ) from final_res.get_fail_cause()  # use the get_fail_cause to get the original exception.
                raise RuntimeError(f'Loading report {self.table} failed with unexpected error.')
            # if dataframe is None, or dataframe is empty and raise_on_empty is True, raise an exception
            if self.raise_on_empty and (final_res.data is None or final_res.data.empty):
                detail_reason = 'a None DataFrame' if final_res.data is None else 'an empty DataFrame'
                raise ValueError(
                    f'Loading report {self.table} on dataset returned {detail_reason}.')
            # If we reach this point, then there is no raise on exceptions, just proceed with
            # wrapping up the report.
            if self.res_container is not None:
                # 1. Append failed apps: Loop on combiner_failed apps and append them to the res_container.
                if self.combiner.failed_loads:
                    for app_id, app_error in self.combiner.failed_loads.items():
                        self.res_container.append_failure(app_id, self.table, app_error)
                # 2. Append the resulting dataframe to the reports if it is successful. Else set the Dataframe to
                # None.
                if final_res.success:
                    self.res_container.append_success(self.table, final_res)
            return final_res.data
        except Exception as e:  # pylint: disable=broad-except
            # If we reach here, it means that the combiner failed to build the final result.
            # We should raise an exception to inform the caller.
            raise RuntimeError(f'Failed to create combined report {self.table}: {e}') from e


class ResHandlerMeta(type):
    """
    Metaclass for ToolResultHandlerT to dynamically create instances. This design allows the user
    to create instances of APIResHandler subclasses without needing to know the specific subclass
    at runtime. The metaclass intercepts the instantiation call, checks the REPORT_LABEL of the
    subclass, and if it is not empty (indicating it's a concrete implementation), it automatically
    calls the report() and build() methods to set up the instance. This approach simplifies the
    instantiation process for users, as they can create fully configured instances of various
    result handlers (e.g., profiling, qualification) without needing to manually call additional
    methods after instantiation.
    """
    def __call__(
            cls,
            out_path: Union[str, BoundedCspPath],
            raise_on_empty: Optional[bool] = False
    ) -> 'APIResHandler[ToolResultHandlerT]':
        instance: 'APIResHandler[ToolResultHandlerT]' = (
            super(ResHandlerMeta, cls).__call__(_out_path=out_path)
        )
        if cls.REPORT_LABEL == '':
            # this is the base class, we do not need to build it
            return instance
        instance = instance.report(cls.REPORT_LABEL).build()
        if raise_on_empty:
            if instance.handler.is_empty():
                raise RuntimeError(f'The result handler {instance.report_id} is empty.')
        return instance


@dataclass
class APIResHandler(Generic[ToolResultHandlerT], metaclass=ResHandlerMeta):
    """
    Generic builder and factory for API v1 result handlers.

    This class provides a unified interface for creating, configuring, and accessing
    different types of report handlers (such as profiling and qualification) based on
    report IDs and output paths. It supports dynamic instantiation via its metaclass,
    delegation to underlying handler methods, and convenient creation of report objects
    (CSV, TXT, JSON properties).

    Attributes:
        REPORT_LABEL (str): The label identifying the report type.
        _out_path (str or BoundedCspPath): Output path for the report.
        _report_id (str): The report ID.
        _res_h (ToolResultHandlerT): The underlying result handler instance.
    """
    REPORT_LABEL: ClassVar[str] = ''
    _out_path: Optional[Union[str, BoundedCspPath]] = None
    _report_id: Optional[str] = None
    _res_h: Optional[ToolResultHandlerT] = field(default=None, init=False)

    @classmethod
    def find_report_paths(
            cls,
            root_path: Union[str, BoundedCspPath],
            filter_cb: Optional[Callable[[BoundedCspPath], bool]] = None
    ) -> Union[List[str], List[BoundedCspPath]]:
        """
        Find paths for a given report type. This method searches only the immediate children of the
        specified root_path and does not perform a recursive search into subdirectories.

        :param root_path: The root directory path or BoundedCspPath to search for report files.
        :param filter_cb: Optional callback to filter BoundedCspPath objects. If provided, only paths
                          for which this callback returns True are included.
        :return: A list of report paths (as str or BoundedCspPath) matching the report type and filter
                 criteria.
        """
        impl_class = result_registry.get(cls.REPORT_LABEL)
        return impl_class.Meta.find_report_paths(
            root_path=root_path,
            filter_cb=filter_cb
        )

    @classmethod
    def from_id(
            cls,
            report_id: str,
            out_path: Union[str, BoundedCspPath]) -> 'APIResHandler[ToolResultHandlerT]':
        """
        Factory method to create an instance of the appropriate subclass based on the provided report ID.
        This is useful if the caller does not know the specific subclass to instantiate. The method
        looks up the report ID in the result_registry and creates an instance of the corresponding subclass.
        Example use-case is to create a handler from string dynamically in the tests.

        :param report_id: The report ID to match against registered subclasses.
        :param out_path: The output path for the result handler.
        :return: An instance of the matching subclass of APIResHandler.
        :raises ValueError: If no matching subclass is found for the given report ID.
        """
        impl_cls = result_registry.get(report_id)
        if impl_cls is None:
            raise ValueError(f'Unknown implementation for report ID: {report_id}')
        # call the metaclass constructor to create the instance
        instance = type(cls).__call__(cls, out_path=out_path)
        # set the report ID and build the handler
        instance = instance.report(report_id).build()
        return instance

    def report(self, rep_id: str) -> 'APIResHandler[ToolResultHandlerT]':
        """Set the report ID for the API v1 components."""
        if not rep_id:
            raise ValueError('Report ID cannot be empty.')
        self._report_id = rep_id
        return self

    def build(self) -> 'APIResHandler[ToolResultHandlerT]':
        if self._report_id is None:
            raise ValueError('Report ID must be set before building.')
        if self._out_path is None:
            raise ValueError('Output path must be set before building.')
        repo_defn = ReportLoader()
        self._res_h = repo_defn.create_result_handler(self._report_id, self._out_path)
        return self

    def _check_handler(self) -> None:
        if self._res_h is None:
            raise ValueError('Result handler is not built yet.')

    @property
    def report_id(self) -> Optional[str]:
        return self._report_id

    @property
    def handler(self) -> ToolResultHandlerT:
        return self._res_h

    def txt(self, tbl: str) -> 'TXTReport':
        """Create a TXTReport for the given table."""
        self._check_handler()
        return TXTReport(self._res_h).table(tbl)

    def csv(self, tbl: str) -> 'CSVReport':
        """Create a CSVReport for the given table."""
        self._check_handler()
        return CSVReport(self._res_h).table(tbl)

    def csv_combiner(self, tbl: str) -> 'CombinedCSVBuilder':
        """Create a CombinedCSVBuilder for the given table."""
        self._check_handler()
        return CombinedCSVBuilder(tbl, self._res_h)

    def jprop(self, tbl: str) -> 'JPropsReport':
        """Create a JPropsReport for the given table."""
        self._check_handler()
        return JPropsReport(self._res_h).table(tbl)

    def json(self, tbl: str) -> 'JSONReport':
        """Create a JSONReport for the given table."""
        self._check_handler()
        return JSONReport(self._res_h).table(tbl)

    ##########################################################################
    # Delegation to the underlying handler
    # Later, this will be replaced by a dynamic proxy to delegate selected calls
    ############################################################################

    @property
    def out_path(self) -> Optional[BoundedCspPath]:
        if self._res_h is None:
            return None
        return self._res_h.out_path

    def is_empty(self) -> bool:
        return self._res_h.is_empty()

    def get_raw_metrics_path(self) -> Optional[BoundedCspPath]:
        return self._res_h.get_raw_metrics_path()


@dataclass
class ProfWrapper(APIResHandler[ProfWrapperResultHandler]):
    REPORT_LABEL: ClassVar[str] = 'profWrapperOutput'


@dataclass
class ProfCore(APIResHandler[ProfCoreResultHandler]):
    REPORT_LABEL: ClassVar[str] = 'profCoreOutput'


@dataclass
class QualWrapper(APIResHandler[QualWrapperResultHandler]):
    REPORT_LABEL: ClassVar[str] = 'qualWrapperOutput'


@dataclass
class QualCore(APIResHandler[QualCoreResultHandler]):
    REPORT_LABEL: ClassVar[str] = 'qualCoreOutput'
