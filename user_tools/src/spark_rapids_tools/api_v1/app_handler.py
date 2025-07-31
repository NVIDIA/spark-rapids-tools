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

from dataclasses import dataclass, field
from functools import cached_property
from typing import Optional

import pandas as pd


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
