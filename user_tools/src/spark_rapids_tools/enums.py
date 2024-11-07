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
import hashlib
from enum import Enum, auto
from typing import Union, cast, Callable


class EnumeratedType(str, Enum):
    """Abstract representation of enumerated values"""

    @classmethod
    def get_default(cls) -> 'EnumeratedType':
        pass

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
    def fromstring(cls, value: str) -> 'EnumeratedType':
        """Return the state object attribute that matches the given value
        :param str value: string to look up
        :return: the state object attribute that matches the string
        :rtype: EnumeratedType
        """
        attribute = getattr(cls, value.upper(), None)
        if attribute is None:
            raise ValueError(f'{value} is not a valid {cls.__name__}')
        return attribute

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

###############
# Utility Enums
###############


class HashAlgorithm(EnumeratedType):
    """Represents the supported hashing algorithms"""
    MD5 = 'md5'
    SHA1 = 'sha1'
    SHA256 = 'sha256'
    SHA512 = 'sha512'

    @classmethod
    def get_default(cls):
        return cls.SHA256

    @classmethod
    def _missing_(cls, value):
        value = value.lower()
        for member in cls:
            if member.lower() == value:
                return member
        return None

    def get_hash_func(self) -> Callable:
        """Maps the hash function to the appropriate hashing algorithm."""
        hash_functions = {
            self.MD5: hashlib.md5,
            self.SHA1: hashlib.sha1,
            self.SHA256: hashlib.sha256,
            self.SHA512: hashlib.sha512,
        }
        return hash_functions[self]


class DependencyType(EnumeratedType):
    """Represents the dependency type for the tools' java cmd."""
    JAR = 'jar'
    ARCHIVE = 'archive'

    @classmethod
    def get_default(cls) -> 'DependencyType':
        """Returns the default dependency type."""
        return cls.JAR


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
    TOP_CANDIDATES = 'top_candidates'
    ALL = 'all'

    @classmethod
    def get_default(cls):
        return cls.TOP_CANDIDATES


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
        return cls.XGBOOST

    @classmethod
    def create_default_model_args(cls, model_type: str) -> dict:
        """
        Giving a estimation-model, it sets the default arguments for the model.
        This is useful to avoid duplicating code all over the place.
        :param model_type: the instance of the estimation-model
        :return: a dictionary with the default arguments for the estimationModel
        """
        return {
            'estimationModel': model_type,
            'xgboostEnabled': model_type == QualEstimationModel.XGBOOST,
            'customModelFile': None,
        }
