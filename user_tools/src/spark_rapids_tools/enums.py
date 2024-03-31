# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

"""Enumeration types commonly used through the AS python implementations."""

from enum import Enum, auto
from typing import Union, cast, Optional, Callable


class EnumeratedType(str, Enum):
    """Abstract representation of enumerated values"""

    # Make enum case-insensitive by overriding the Enum's missing method
    @classmethod
    def _missing_(cls, value):
        value = value.lower()
        for member in cls:
            if member.lower() == value:
                return member
        return None

    @classmethod
    def tostring(cls, value: Union[Enum, str]) -> str:
        """Return the string representation of the state object attribute
        :param str value: the state object to turn into string
        :return: the uppercase string that represents the state object
        :rtype: str
        """
        value = cast(Enum, value)
        return str(value._value_).upper()  # pylint: disable=protected-access

    @classmethod
    def fromstring(cls, value: str) -> Optional[str]:
        """Return the state object attribute that matches the given value
        :param str value: string to look up
        :return: the state object attribute that matches the string
        :rtype: str
        """
        return getattr(cls, value.upper(), None)

    @classmethod
    def pretty_print(cls, value):
        # type: (Union[Enum, str]) -> str
        """Return the string representation of the state object attribute
        :param str value: the state object to turn into string
        :return: the string that represents the state object
        :rtype: str
        """
        value = cast(Enum, value)
        return str(value._value_)  # pylint: disable=protected-access


###########
# CSP Enums
###########

class CspEnv(EnumeratedType):
    """Represents the supported types of runtime CSP"""
    DATABRICKS_AWS = 'databricks_aws'
    DATABRICKS_AZURE = 'databricks_azure'
    DATAPROC = 'dataproc'
    DATAPROC_GKE = 'dataproc_gke'
    EMR = 'emr'
    ONPREM = 'onprem'
    NONE = 'NONE'

    @classmethod
    def get_default(cls):
        return cls.ONPREM

    @classmethod
    def _missing_(cls, value):
        value = value.lower()
        # convert hyphens to underscores
        value = value.replace('-', '_')
        for member in cls:
            if member.lower() == value:
                return member
        return None

    @classmethod
    def requires_pricing_map(cls, value) -> bool:
        return value in [cls.ONPREM]

    def get_equivalent_pricing_platform(self) -> list:
        platforms_map = {
            self.ONPREM: [CspEnv.DATAPROC]
        }
        return platforms_map.get(self)

    def map_to_java_arg(self) -> str:
        str_value = self.__class__.pretty_print(self)
        # convert_underscores_to-hyphens
        return str_value.replace('_', '-')


#############
# Tools Enums
#############

class QualFilterApp(EnumeratedType):
    """Values used to filter out the applications in the qualification report"""
    SAVINGS = 'savings'
    SPEEDUPS = 'speedups'
    TOP_CANDIDATES = 'top_candidates'
    ALL = 'all'

    @classmethod
    def get_default(cls):
        return cls.TOP_CANDIDATES


class QualGpuClusterReshapeType(EnumeratedType):
    """Values used to filter out the applications in the qualification report"""
    MATCH = 'match'
    CLUSTER = 'cluster'
    JOB = 'job'

    @classmethod
    def get_default(cls):
        return cls.MATCH


class ConditionOperator(EnumeratedType):
    """Enum representing comparison operators for conditions."""
    EQUAL = auto()
    NOT_EQUAL = auto()
    GREATER_THAN = auto()
    LESS_THAN = auto()
    GREATER_THAN_OR_EQUAL = auto()
    LESS_THAN_OR_EQUAL = auto()

    @classmethod
    def get_operator_fn(cls, operator: str) -> Callable[[any, any], bool]:
        """
        Returns the operator function for a given operator input.
        """
        operator_functions = {
            cls.EQUAL: lambda x, y: x == y,
            cls.NOT_EQUAL: lambda x, y: x != y,
            cls.GREATER_THAN: lambda x, y: x > y,
            cls.LESS_THAN: lambda x, y: x < y,
            cls.GREATER_THAN_OR_EQUAL: lambda x, y: x >= y,
            cls.LESS_THAN_OR_EQUAL: lambda x, y: x <= y,
        }
        try:
            return operator_functions[ConditionOperator.fromstring(operator)]
        except (KeyError, ValueError) as e:
            raise ValueError(f'Operator function not defined for {operator}') from e


class QualEstimationModel(EnumeratedType):
    """Values used to define the speedup values of the applications"""
    XGBOOST = 'xgboost'
    SPEEDUPS = 'speedups'

    @classmethod
    def get_default(cls):
        return cls.SPEEDUPS
