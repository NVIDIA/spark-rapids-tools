# Copyright (c) 2023, NVIDIA CORPORATION.
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

"""Implementation of helpers and utilities related to manage the properties and dictionaries."""

import json
from dataclasses import field, dataclass
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Callable

import yaml

from spark_rapids_tools import get_elem_from_dict, get_elem_non_safe


def convert_dict_to_camel_case(dic: dict):
    """
    Given a dictionary with underscore keys. This method converts the keys to a camelcase.
    Example, gce_cluster_config -> gceClusterConfig
    :param dic: the dictionary to be converted
    :return: a dictionary where all the keys are camelcase.
    """
    def to_camel_case(word: str) -> str:
        return word.split('_')[0] + ''.join(x.capitalize() or '_' for x in word.split('_')[1:])

    if isinstance(dic, list):
        return [convert_dict_to_camel_case(i) if isinstance(i, (dict, list)) else i for i in dic]
    res = {}
    for key, value in dic.items():
        if isinstance(value, (dict, list)):
            res[to_camel_case(key)] = convert_dict_to_camel_case(value)
        else:
            res[to_camel_case(key)] = value
    return res


@dataclass
class AbstractPropertiesContainer(object):
    """
    An abstract class that loads properties (dictionary).
    """
    prop_arg: str
    file_load: bool = True
    props: Any = field(default=None, init=False)

    def apply_conversion(self, func_cb: Callable):
        self.props = func_cb(self.props)

    def get_value(self, *key_strs):
        return get_elem_from_dict(self.props, key_strs)

    def get_value_silent(self, *key_strs):
        return get_elem_non_safe(self.props, key_strs)

    def _init_fields(self):
        pass

    def _load_properties_from_file(self):
        """
        In some case, we want to be able to accept both json and yaml format when the properties are saved as a file.
        :return:
        """
        file_suffix = Path(self.prop_arg).suffix
        if file_suffix in ('.yaml', '.yml'):
            # this is a yaml property
            self.__open_yaml_file()
        else:
            # this is a jso file
            self.__open_json_file()

    def __open_json_file(self):
        try:
            with open(self.prop_arg, 'r', encoding='utf-8') as json_file:
                try:
                    self.props = json.load(json_file)
                except JSONDecodeError as e:
                    raise RuntimeError('Incorrect format of JSON File') from e
                except TypeError as e:
                    raise RuntimeError('Incorrect Type of JSON content') from e
        except OSError as err:
            raise RuntimeError('Please ensure the json file exists '
                               'and you have the required access privileges.') from err

    def __open_yaml_file(self):
        try:
            with open(self.prop_arg, 'r', encoding='utf-8') as yaml_file:
                try:
                    self.props = yaml.safe_load(yaml_file)
                except yaml.YAMLError as e:
                    raise RuntimeError('Incorrect format of Yaml File') from e
        except OSError as err:
            raise RuntimeError('Please ensure the properties file exists '
                               'and you have the required access privileges.') from err

    def _load_as_yaml(self):
        if self.file_load:
            # this is a file argument
            self._load_properties_from_file()
        else:
            try:
                self.props = yaml.safe_load(self.prop_arg)
            except yaml.YAMLError as e:
                raise RuntimeError('Incorrect format of Yaml File') from e

    def _load_as_json(self):
        if self.file_load:
            # this is a file argument
            self._load_properties_from_file()
        else:
            try:
                if isinstance(self.prop_arg, str):
                    self.props = json.loads(self.prop_arg)
                else:
                    self.props = self.prop_arg
            except JSONDecodeError as e:
                raise RuntimeError('Incorrect format of JSON File') from e
            except TypeError as e:
                raise RuntimeError('Incorrect Type of JSON content') from e


@dataclass
class YAMLPropertiesContainer(AbstractPropertiesContainer):

    def __post_init__(self):
        self._load_as_yaml()
        self._init_fields()


@dataclass
class JSONPropertiesContainer(AbstractPropertiesContainer):

    def __post_init__(self):
        self._load_as_json()
        self._init_fields()
