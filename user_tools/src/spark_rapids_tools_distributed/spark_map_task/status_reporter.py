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

""" Module for reporting the status of applications. """

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict


class AppStatus(Enum):
    """
    Enumerated type for the status of an application.
    """
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'


@dataclass
class AppStatusResult:
    """
    Class for storing the status of an application.
    """
    eventlog_path: str = field(default=None, init=True)
    status: AppStatus = field(default=None, init=False)
    description: str = field(default='', init=True)

    # Keys for the dictionary representation
    eventlog_key: str = 'Event Log'
    status_key: str = 'Status'
    description_key: str = 'Description'

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the instance to a dictionary.
        """
        return {
            self.eventlog_key: self.eventlog_path,
            self.status_key: self.status.value,
            self.description_key: self.description
        }


@dataclass
class FailureAppStatus(AppStatusResult):
    """
    Class for storing the status of a failed application.
    """
    status: AppStatus = AppStatus.FAILURE


@dataclass
class SuccessAppStatus(AppStatusResult):
    """
    Class for storing the status of a successful application.
    """
    status: AppStatus = AppStatus.SUCCESS
