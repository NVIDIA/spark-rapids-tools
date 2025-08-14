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

"""module that defines the app descriptor for the results loaded by the tools."""
import re
from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional, Dict, List

import pandas as pd

from spark_rapids_tools.api_v1 import APIUtils
from spark_rapids_tools.utils import Utilities


@dataclass
class AppHandler(object):
    """A wrapper tha represents a single run analyzed by the tools."""
    _app_id: str
    # represent the attempt-number
    _attempt_id: Optional[int] = field(default=1)
    # the app-name
    _app_name: Optional[str] = field(default='UNKNOWN_APP')
    # this will be loaded from the core-status csv report
    eventlog_path: Optional[str] = None

    def is_name_defined(self) -> bool:
        """
        Check if the app name is defined.
        :return: True if the app name is defined, False otherwise.
        """
        return self._app_name != 'UNKNOWN_APP'

    def is_attempt_defined(self) -> bool:
        """
        Check if the attempt ID is defined.
        :return: True if the attempt ID is defined, False otherwise.
        """
        return self._attempt_id != 1

    def patch_into_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Given a dataframe, this method will stitch the app_id and app-name to the dataframe.
        This can be useful in automatically adding the app-id/app-name to the data-frame
        :param df: the dataframe that we want to modify.
        :return: the resulting dataframe from adding the columns.
        """
        if not df.empty:
            # TODO: We should consider add UUID as well, and use that for the joins instead.
            df.insert(0, 'attemptId', self._attempt_id)
            df.insert(0, 'appId', self._app_id)
        return df

    @property
    def app_id(self) -> str:
        """Get the app ID."""
        return self._app_id

    @app_id.setter
    def app_id(self, value: str) -> None:
        """Set the app ID and refresh cached UUID."""
        self._app_id = value
        # Clear cached property to force recalculation
        if 'uuid' in self.__dict__:
            del self.__dict__['uuid']

    @property
    def attempt_id(self) -> int:
        """Get the attempt ID."""
        return self._attempt_id

    @attempt_id.setter
    def attempt_id(self, value: int) -> None:
        """Set the attempt ID and refresh cached UUID."""
        self._attempt_id = value
        # Clear cached property to force recalculation
        if 'uuid' in self.__dict__:
            del self.__dict__['uuid']

    @property
    def app_name(self) -> str:
        """Get the app_name."""
        return self._app_name

    @app_name.setter
    def app_name(self, value: str) -> None:
        """Set the attempt ID and refresh cached UUID."""
        self._app_name = value

    def merge(self, other: 'AppHandler') -> 'AppHandler':
        """
        Fix the missing data in an AppHandler by merging the information from 2 different
        candidates.
        :param other: the other app handler that we want to normalize to.
        :return: a new AppHandler that is normalized to the other app handler.
        """
        # we should make sure that the app_id is the same
        if not self.is_name_defined() and other.is_name_defined():
            self.app_name = other.app_name
        if not self.is_attempt_defined() and other.is_attempt_defined():
            self.attempt_id = other.attempt_id
        if self.eventlog_path is None and other.eventlog_path is not None:
            self.eventlog_path = other.eventlog_path
        return self

    ################################
    # Public Methods
    ################################

    @cached_property
    def uuid(self) -> str:
        """
        This is the uuid that represents the app_handler.
        This should be the only way to represent an app.
        # Later, in order to support multi-attempts, this method will simply
        # be converted to concatenate the attempt-id.
        :return: a string uuid for a single app.
        """
        return self._app_id

    def convert_to_df(self) -> pd.DataFrame:
        """
        Convert the AppHandler attributes to a DataFrame.
        :return: DataFrame with app_id, app_name, and attempt_id as columns.
        """
        data = {
            'app_id': [self.app_id],
            'attempt_id': [self.attempt_id],
            'app_name': [self.app_name],
            'eventlog_path': [self.eventlog_path]
        }
        data_types = AppHandler.get_pd_dtypes()
        return pd.DataFrame({
            col: pd.Series(data[col], dtype=dtype) for col, dtype in data_types.items()
        })

    def add_fields_to_dataframe(self,
                                df: pd.DataFrame,
                                field_to_col_map: Dict[str, str]) -> pd.DataFrame:
        """
        Insert fields/properties from AppHandler into the DataFrame, with user-specified column names.
        :param df: Existing DataFrame to append to.
        :type df: pd.DataFrame
        :param field_to_col_map: Dictionary mapping AppHandler attributes (keys) to DataFrame column names (values).
        :type field_to_col_map: Dict[str, str]
        default: Value to use if attribute/property not found (raises if None).
        """
        converted_df = self.convert_to_df()
        row_data = []
        for attr, col in field_to_col_map.items():
            # Normalize the attribute name
            norm_attr = AppHandler.normalize_attribute(attr)
            try:
                value = getattr(self, norm_attr)
                row_data.append((col, norm_attr, value))
            except AttributeError as exc:
                raise AttributeError(f"Attribute '{attr}' not found in AppHandler.") from exc
        for col, norm_attr, value in reversed(row_data):
            # Check if the column already exists in the DataFrame
            if col in df.columns:
                # If it exists, we should not overwrite it, skip
                continue
            # create a new column with the correct type. We do this because we do not want to
            # to add values to an empty dataframe.
            df.insert(loc=0, column=col, value=pd.Series(dtype=converted_df[norm_attr].dtype))
            # set the values in case the dataframe was non-empty.
            df[col] = pd.Series([value] * len(df), dtype=converted_df[norm_attr].dtype)
        return df

    @classmethod
    def inject_into_df(cls,
                       df: pd.DataFrame,
                       field_to_col_map: Dict[str, str],
                       app_h: Optional['AppHandler'] = None) -> pd.DataFrame:
        """
        Inject AppHandler fields into a DataFrame using a mapping of field names to column names.
        :param df:
        :param field_to_col_map:
        :param app_h:
        :return:
        """
        if app_h is None:
            app_h = AppHandler(_app_id='UNKNOWN_APP', _app_name='UNKNOWN_APP', _attempt_id=1)
        return app_h.add_fields_to_dataframe(df, field_to_col_map)

    @classmethod
    def get_key_attributes(cls) -> List[str]:
        """
        Get the key attributes that define an AppHandler.
        :return: List of key attributes.
        """
        return ['app_id']

    @classmethod
    def get_default_key_columns(cls) -> Dict[str, str]:
        """
        Get the default key columns for the AppHandler.
        :return: Dictionary mapping attribute names to column names.
        """
        res = {}
        for attr in cls.get_key_attributes():
            res[attr] = Utilities.str_to_camel(attr)
        return res

    @staticmethod
    def normalize_attribute(arg_value: str) -> str:
        """
        Normalize the attribute name to a plain format.
        It uses re.sub to replace any '-' or '_' with a space using the regexp 'r"(_|-)+"'.
        Finally, it uses str.replace() to remove any spaces.
        :param arg_value: the attribute name to normalize.
        :return: the actual field name that is used in the AppHandler.
        """
        processed_value = re.sub(r'([_\-])+', ' ', arg_value.strip().lower()).replace(' ', '')
        lookup_map = {
            'appname': 'app_name',
            'appid': 'app_id',
            'attemptid': 'attempt_id',
            'eventlogpath': 'eventlog_path'
        }
        return lookup_map.get(processed_value, arg_value)

    @staticmethod
    def get_pd_dtypes() -> Dict[str, str]:
        """
        Get the pandas data types for the AppHandler attributes.
        :return: Dictionary mapping attribute names to pandas data types.
        """
        return {
            'app_id': APIUtils.scala_to_pd_dtype('String'),
            'attempt_id': APIUtils.scala_to_pd_dtype('Int'),
            'app_name': APIUtils.scala_to_pd_dtype('String'),
            'eventlog_path': APIUtils.scala_to_pd_dtype('String')
        }

    @staticmethod
    def normalize_app_id_col(col_names: List[str]) -> Dict[str, str]:
        """
        Normalize the appId column name to 'appId' if it exists in the column names.
        :param col_names: List of column names.
        :return: A dictionary mapping the original column name to 'appId' if it exists.
        """
        for col_name in col_names:
            if AppHandler.normalize_attribute(col_name) == 'app_id':
                return {col_name: 'appId'}
        return {}
