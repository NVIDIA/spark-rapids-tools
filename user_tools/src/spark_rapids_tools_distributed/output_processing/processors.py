# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Module to define classes for processing metrics files. """

import fnmatch
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from logging import Logger
from typing import Dict, List

import pandas as pd
import pyarrow

from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools import CspPath
from spark_rapids_tools.storagelib import HdfsPath, CspFs


@dataclass
class OutputProcessor(ABC):
    """
    Base class for all output processors
    """
    cli_jar_output_path: CspPath = field(default=None, init=True)
    logger: Logger = field(default=None, init=False)

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed.output_processor')

    @abstractmethod
    def process(self, app_metric_path: HdfsPath, **kwargs) -> None:
        """
        Abstract method for processing files.
        """
        raise NotImplementedError


@dataclass
class CopyOutputProcessor(OutputProcessor):
    """
    Class to process files by copying them to the output folder.
    """

    def process(self, app_metric_path: HdfsPath, **kwargs) -> None:
        resource_name = kwargs.get('resource_name')
        resource_path = app_metric_path.create_sub_path(resource_name)
        if resource_path.exists():
            CspFs.copy_resources(src=resource_path, dest=self.cli_jar_output_path)


@dataclass
class MergeOutputProcessor(OutputProcessor):
    """
    Abstract class to process files and merge output from multiple executors.
    """
    combined_data_dict: Dict[str, List] = field(default_factory=dict, init=False)
    pattern: str = field(default=None, init=False)

    @staticmethod
    def get_matching_files(app_metric_path: HdfsPath, pattern: str) -> List[HdfsPath]:
        """
        Get files matching a given pattern in the directory.
        """
        return [info for info in CspFs.list_all_files(app_metric_path)
                if fnmatch.fnmatch(info.base_name(), pattern)]

    def process(self, app_metric_path: HdfsPath, **kwargs) -> None:
        """
        Process files and add data to the combined data dictionary.
        """
        files = self.get_matching_files(app_metric_path, pattern=self.pattern)
        if not files:
            self.logger.warning('No files found matching pattern %s in %s', self.pattern, app_metric_path)
            return

        for file_info in files:
            file_name = file_info.base_name()
            try:
                with file_info.open_input_stream() as file:
                    content = self.read_file(file)
                    if file_name not in self.combined_data_dict:
                        self.combined_data_dict[file_name] = []
                    self.combined_data_dict[file_name].append(content)
            except Exception as e:  # pylint: disable=broad-except
                raise RuntimeError(f'Error processing file {file_name}: {e}') from e

    def write_data(self) -> None:
        """
        Write the combined data to the output folder.
        """
        for filename, data in self.combined_data_dict.items():
            combined_data = self.combine_data(data)
            output_path = self.cli_jar_output_path.create_sub_path(filename)
            try:
                with output_path.open_output_stream() as file:
                    self.write_combined_data(combined_data, file)
            except Exception as e:
                raise RuntimeError(f'Error writing file {output_path}: {e}') from e

    @abstractmethod
    def read_file(self, file: pyarrow.NativeFile) -> object:
        """
        Abstract method to read file contents.
        """
        raise NotImplementedError

    @abstractmethod
    def combine_data(self, data_list: List[object]) -> object:
        """
        Abstract method to combine data from multiple executors.
        """
        raise NotImplementedError

    @abstractmethod
    def write_combined_data(self, combined_data: object, file: pyarrow.NativeFile) -> None:
        """
        Abstract method to write the combined data to the output file.
        """
        raise NotImplementedError


class CSVOutputProcessor(MergeOutputProcessor):
    """
    Class to implement processing of CSV files.
    """
    pattern = '*.csv'

    def read_file(self, file: pyarrow.NativeFile) -> pd.DataFrame:
        return pd.read_csv(file)

    def combine_data(self, data_list: List[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(data_list, ignore_index=True)

    def write_combined_data(self, combined_data: pd.DataFrame, file: pyarrow.NativeFile) -> None:
        combined_data.to_csv(file, index=False)


class JSONOutputProcessor(MergeOutputProcessor):
    """
    Class to implement processing of JSON files.
    """
    pattern = '*.json'

    def read_file(self, file: pyarrow.NativeFile) -> List[dict]:
        data = json.load(file)
        if not (isinstance(data, list) and all(isinstance(item, dict) for item in data)):
            raise ValueError('Unexpected format of JSON data in file')
        return data

    def combine_data(self, data_list: List[List[dict]]) -> List[dict]:
        return [item for data in data_list for item in data]

    def write_combined_data(self, combined_data: List[dict], file: pyarrow.NativeFile) -> None:
        json_str = json.dumps(combined_data, indent=2)
        file.write(json_str.encode('utf-8'))


class LogOutputProcessor(MergeOutputProcessor):
    """
    Class to implement processing of log files
    """
    pattern = '*.log'

    def read_file(self, file: pyarrow.NativeFile) -> str:
        return file.read().decode('utf-8')

    def combine_data(self, data_list: List[str]) -> str:
        return '\n'.join(data_list)

    def write_combined_data(self, combined_data: str, file: pyarrow.NativeFile) -> None:
        file.write(combined_data.encode('utf-8'))
