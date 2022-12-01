# Copyright (c) 2022, NVIDIA CORPORATION.
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

"""Definition of global utilities and helpers methods."""

import json
import logging
import os
import secrets
import string
import subprocess
import sys
from dataclasses import dataclass, field
from functools import reduce
from json import JSONDecodeError
from operator import getitem
from pathlib import Path
from shutil import rmtree, which
from typing import Any

import yaml


def get_log_dict(args):
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '{asctime} {levelname} {name}: {message}',
                'style': '{',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
            },
        },
        'root': {
            'handlers': ['console'],
            'level': 'DEBUG' if args.get('debug') else 'INFO',
        },
    }


logger = logging.getLogger(__name__)


def bail(msg, err):
    """
    Print message and the error before terminating the program.
    :param msg: message error to display.
    :param err: the Error/Exception that caused the failure.
    :return: NONE
    """
    print(f'{msg}.\n\t> {err}.\nTerminated.')
    sys.exit(1)


def get_elem_from_dict(data, keys):
    try:
        return reduce(getitem, keys, data)
    except LookupError:
        print(f'ERROR: Could not find elements [{keys}]')
        return None


def get_elem_non_safe(data, keys):
    try:
        return reduce(getitem, keys, data)
    except LookupError:
        return None


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


def gen_random_string(str_length: int) -> str:
    return ''.join(secrets.choice(string.hexdigits) for _ in range(str_length))


def get_gpu_device_list():
    return ['T4', 'V100', 'K80', 'A100', 'P100']


def is_valid_gpu_device(val):
    return val.upper() in get_gpu_device_list()


def get_gpu_short_name(val: str) -> str:
    """
    Given a value string, return the short name of the GPU device.
    :param val: the full name example nvidia-tesla-t4
    :return: the shortname of the GPU device (T4). otherwise, None.
    """
    upper_full_name = val.upper()
    for short_name in get_gpu_device_list():
        if upper_full_name.find(short_name) != -1:
            return short_name
    return None


def is_system_tool(tool_name):
    """
    check whether a tool is installed on the system.
    :param tool_name: name of the tool to check
    :return: True or False
    """
    return which(tool_name) is not None


def remove_dir(dir_path: str, fail_on_error: bool = True):
    try:
        rmtree(dir_path)
    except OSError as error:
        if fail_on_error:
            bail(f'Could not remove directory {dir_path}', error)


def make_dirs(dir_path: str, exist_ok: bool = True):
    try:
        os.makedirs(dir_path, exist_ok=exist_ok)
    except OSError as error:
        bail(f'Error Creating directories {dir_path}', error)


def resource_path(resource_name: str) -> str:
    # pylint: disable=import-outside-toplevel
    if sys.version_info < (3, 9):
        import importlib_resources
    else:
        import importlib.resources as importlib_resources

    pkg = importlib_resources.files('spark_rapids_dataproc_tools')
    return pkg / 'resources' / resource_name


@dataclass
class AbstractPropertiesContainer(object):
    """
    An abstract class that loads properties (dictionary).
    """
    prop_arg: str
    file_load: bool = True
    props: Any = field(default=None, init=False)

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
                    bail('Incorrect format of JSON File', e)
                except TypeError as e:
                    bail('Incorrect Type of JSON content', e)
        except OSError as err:
            bail('Please ensure the json file exists and you have the required access privileges.', err)

    def __open_yaml_file(self):
        try:
            with open(self.prop_arg, 'r', encoding='utf-8') as yaml_file:
                try:
                    self.props = yaml.safe_load(yaml_file)
                except yaml.YAMLError as e:
                    bail('Incorrect format of Yaml File', e)
        except OSError as err:
            bail('Please ensure the properties file exists and you have the required access privileges.', err)

    def _load_as_yaml(self):
        if self.file_load:
            # this is a file argument
            self._load_properties_from_file()
        else:
            try:
                self.props = yaml.safe_load(self.prop_arg)
            except yaml.YAMLError as e:
                bail('Incorrect format of Yaml File', e)

    def _load_as_json(self):
        if self.file_load:
            # this is a file argument
            self._load_properties_from_file()
        else:
            try:
                self.props = json.loads(self.prop_arg)
            except JSONDecodeError as e:
                bail('Incorrect format of JSON File', e)
            except TypeError as e:
                bail('Incorrect Type of JSON content', e)


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


def run_cmd(cmd, check=True, capture=''):
    """Run command and check return code, capture output etc."""
    stdout = None
    stderr = None

    if capture:
        if capture == 'stdout':
            stdout = subprocess.PIPE
        elif capture == 'stderr':
            stderr = subprocess.PIPE
        elif capture == 'all':
            stdout, stderr = subprocess.PIPE, subprocess.PIPE
        else:
            raise Exception(f'unknown capture value: {capture}')

    # pylint: disable=subprocess-run-check
    result = subprocess.run(' '.join(cmd), executable='/bin/bash', shell=True, stdout=stdout, stderr=stderr)
    # pylint: enable=subprocess-run-check
    logger.debug('run_cmd: %s', result)

    if check:
        if result.returncode == 0:
            if stdout and stderr:
                return result.stdout.decode('utf-8'), result.stderr.decode('utf-8')

            if stdout:
                return result.stdout.decode('utf-8')

            if stderr:
                return result.stderr.decode('utf-8')

        else:
            raise Exception(f'run cmd failed: {result}')

    return result
