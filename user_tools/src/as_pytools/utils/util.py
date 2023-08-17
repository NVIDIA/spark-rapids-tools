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

"""Utility and helper methods"""
import os
import pathlib
import re
from functools import reduce
from operator import getitem
from typing import Any, Optional

import fire
from pydantic import ValidationError, AnyHttpUrl, TypeAdapter

from as_pytools.exceptions import CspPathAttributeError


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


def stringify_path(fpath) -> str:
    if isinstance(fpath, str):
        actual_val = fpath
    elif hasattr(fpath, '__fspath__'):
        actual_val = os.fspath(fpath)
    else:
        raise CspPathAttributeError('Not a valid path')

    return os.path.expanduser(actual_val)


def is_http_file(value: Any) -> bool:
    try:
        TypeAdapter(AnyHttpUrl).validate_python(value)
        return True
    except ValidationError:
        # ignore
        return False


def get_path_as_uri(fpath: str) -> str:
    if re.match(r'\w+://', fpath):
        # that's already a valid url
        return fpath
    # stringify the path to apply the common methods which is expanding the file.
    local_path = stringify_path(fpath)
    return pathlib.PurePath(local_path).as_uri()


def to_camel_case(word: str) -> str:
    return word.split('_')[0] + ''.join(x.capitalize() or '_' for x in word.split('_')[1:])


def to_camel_capital_case(word: str) -> str:
    return ''.join(x.capitalize() for x in word.split('_'))


def to_snake_case(word: str) -> str:
    return ''.join(['_' + i.lower() if i.isupper() else i for i in word]).lstrip('_')


def get_tool_usage(tool_name: Optional[str]) -> str:
    imported_module = __import__('as_pytools.cmdli', globals(), locals(), ['ASCLIWrapper'])
    wrapper_clzz = getattr(imported_module, 'ASCLIWrapper')
    usage_cmd = '--help' if tool_name is None else f'{tool_name} --help'
    return fire.Fire(wrapper_clzz(), command=usage_cmd)
