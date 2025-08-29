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

"""Module that contains the definitions of result accessors and handlers"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from functools import cached_property
from logging import Logger
from typing import Dict, Optional, TypeVar, Type, Union, List, Callable

import pandas as pd
from pyarrow.fs import FileType

from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.api_v1 import AppHandler
from spark_rapids_tools.api_v1.report_reader import ToolReportReader
from spark_rapids_tools.storagelib.cspfs import BoundedCspPath, CspFs


class ResultHandlerBaseMeta:    # pylint: disable=too-few-public-methods
    """
    Meta class for ResultHandler to define common attributes.
    """
    id_regex: re.Pattern[str] = None

    @classmethod
    def find_report_paths(
            cls,
            root_path: Union[str, BoundedCspPath],
            filter_cb: Optional[Callable[[BoundedCspPath], bool]] = None
    ) -> Union[List[str], List[BoundedCspPath]]:
        """
        Find all report directory paths under the given root that match the class-defined `id_regex` pattern.
        :param root_path: The root directory or `BoundedCspPath` where reports are stored.
        :param filter_cb: Optional callback to filter the found report paths.
        :return: A list of matching report directory paths as strings or `BoundedCspPath` objects,
                depending on the input type.
        """
        report_csps = CspFs.glob_path(
            path=root_path,
            pattern=cls.id_regex,
            item_type=FileType.Directory,
            recursive=False
        )
        if filter_cb is None:
            filtered_csp_paths = report_csps
        else:
            filtered_csp_paths = [p for p in report_csps if filter_cb(p)]
        if isinstance(root_path, str):
            return [p.to_str_format() for p in filtered_csp_paths]
        return report_csps


@dataclass
class ResultHandler(object):
    """
    A class to handle the results of a tool run, including loading dataframes and managing app
    handlers.
    @param report_id: The unique identifier for the report.
    @param out_path: The output path where the results will be stored.
    @param readers: A dictionary of ToolReportReader instances, each associated with a specific
                    report ID. These readers are responsible for loading and managing the data
    @param logger: An optional logger instance for logging messages. If not provided, a default
                    logger will be created.
    @param app_handlers: A dictionary of AppHandler instances, initialized after the object is
                        created. These handlers manage application-specific data and metadata. This
                        field is not initialized directly and is set up in the `__post_init__` method.
    """
    class Meta(ResultHandlerBaseMeta):    # pylint: disable=too-few-public-methods
        id_regex = re.compile(r'UNDEFINED')

    report_id: str
    out_path: BoundedCspPath
    readers: Dict[str, ToolReportReader]
    logger: Optional[Logger] = field(default=None)
    app_handlers: Dict[str, AppHandler] = field(default_factory=dict, init=False)

    def __post_init__(self):
        # init the logger if it is not defined
        if self.logger is None:
            self.logger = ToolLogging.get_and_setup_logger(self.__class__.__name__)
        self._init_app_handlers()

    def _propagate_app_descs(self,
                             driver_reader: Optional[ToolReportReader] = None) -> None:
        """
        Given a "main" reader object, this method will trigger the resolution of app_fds in that reader.
        Then, it will propagate those fds to other readers.
        :param driver_reader: The main reader that is responsible for loading the app descriptors.
        If None, it will not propagate the app_fds.
        This is useful when the app_fds are already resolved and we want to update the readers
        with the latest app_fds.
        :return: None
        """
        if driver_reader is not None:
            driver_reader.resolve_app_fds()
            self.app_handlers.update(driver_reader.app_fds)
            for reader in self.readers.values():
                if reader.report_id != driver_reader.report_id:
                    reader.set_apps(driver_reader.app_fds)

    def _init_app_handlers(self) -> None:
        """
        Initialize the application handlers by loading the application descriptors from the
        main reader (alpha reader). This method will also propagate the application descriptors
        to all other readers in the report.
        :return: None
        """
        app_loader = self.app_loader_reader
        self._propagate_app_descs(app_loader)

    @property
    def alpha_reader(self) -> ToolReportReader:
        """
        Get the alpha reader, which is the reader that leads the report behavior.
        By default the alpha reader is the reader labeled with same Id as the report_id.
        :return: the alpha reader for the report.
        """
        return self.readers.get(self.report_id)

    @property
    def app_loader_reader(self) -> ToolReportReader:
        """
        Get the reader that is responsible for loading the applications descriptors. This will be
        to all other nested readers if any.
        :return: the reader that loads the application descriptors.
        """
        return self.alpha_reader

    @cached_property
    def tbl_reader_map(self) -> Dict[str, ToolReportReader]:
        result: Dict[str, ToolReportReader] = {}
        for r in self.readers.values():
            for t in r.table_definitions.values():
                result[t.label] = r
        return result

    def is_per_app_tbl(self, tbl: str) -> bool:
        """
        Check if the table is defined per application.
        :param tbl: the label of the table.
        :return: True if the table is defined per application, False otherwise.
        """
        reader = self.tbl_reader_map.get(tbl)
        if reader:
            return reader.is_per_app()
        return False

    def get_reader_by_tbl(self, tbl: str) -> Optional[ToolReportReader]:
        """
        Get the reader for the given table label.
        :param tbl: the unique label of the table.
        :return: the reader for the table, or None if not found.
        """
        return self.tbl_reader_map.get(tbl)

    #########################
    # Public Interfaces
    #########################

    def get_folder_name(self) -> str:
        """
        get the base-name of the folder output. This can be handy to act as an identifier for the
        output processor.
        :return: the basename of the output folder
        """
        return self.out_path.base_name()

    def get_reader_path(self, report_id: str) -> Optional[BoundedCspPath]:
        """
        Get the path to the report file for the given report ID.
        :param report_id: The unique identifier for the report.
        :return: The path to the report file, or None if not found.
        """
        reader = self.readers.get(report_id)
        if reader:
            return reader.out_path
        return None

    def create_empty_df(self, tbl: str) -> pd.DataFrame:
        """
        Create an empty DataFrame for the given table label.
        :param tbl: the unique label of the table.
        :return: an empty DataFrame with the table's schema.
        """
        reader = self.tbl_reader_map.get(tbl)
        tbl = reader.get_table(tbl_lbl=tbl)
        return tbl.create_empty_df()

    def get_table_path(self, table_label: str) -> Optional[BoundedCspPath]:
        """
        Get the path to the table file for the given label.
        :param table_label: The label of the table.
        :return: The path to the table file.
        """
        reader = self.tbl_reader_map.get(table_label)
        if reader:
            return reader.get_table_path(table_label)
        return None

    def is_empty(self) -> bool:
        """
        Check if the result handler has no data.
        :return: True if the result handler is empty, False otherwise.
        """
        # first check that the output file exists
        if not self.out_path.exists():
            return True
        # then check that the app_handlers are empty
        return not self.app_handlers

    def get_raw_metrics_path(self) -> Optional[BoundedCspPath]:
        return self.get_reader_path('coreRawMetrics')

#########################
# Type Definitions
#########################


ToolResultHandlerT = TypeVar('ToolResultHandlerT', bound=ResultHandler)
result_registry: Dict[str, Type[ToolResultHandlerT]] = defaultdict(lambda: ResultHandler)


def register_result_class(key: str):
    """
    A decorator to register dataclasses with a custom key.
    """
    def decorator(cls: Type[ToolResultHandlerT]) -> Type[ToolResultHandlerT]:
        result_registry[key] = cls
        return cls
    return decorator
