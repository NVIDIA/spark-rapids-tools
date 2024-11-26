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

"""
Define some custom exceptions defined through the implementation.
This helps to catch specific behaviors when necessary
"""

from typing import Optional

from pydantic import ValidationError


class CspPathException(Exception):
    """Base exception for all custom exceptions."""


class InvalidProtocolPrefixError(CspPathException, ValueError):
    pass


class FSMismatchError(CspPathException, ValueError):
    pass


class CspFileExistsError(CspPathException, ValueError):
    pass


class CspPathNotFoundException(CspPathException, ValueError):
    pass


class JsonLoadException(CspPathException, ValueError):
    pass


class YamlLoadException(CspPathException, ValueError):
    pass


class CspPathAttributeError(CspPathException, ValueError):
    pass


class CspPathTypeMismatchError(CspPathException, ValueError):
    pass


class InvalidPropertiesSchema(CspPathException, ValueError):
    """
    Defines a class to represent errors caused by invalid properties schema
    """
    def __init__(self, msg: str, pydantic_err: Optional[ValidationError] = None):
        if pydantic_err is None:
            self.message = msg
        else:
            content = [msg]
            for err_obj in pydantic_err.errors():
                field_loc = err_obj.get('loc')
                field_title = field_loc[0] if field_loc else ''
                single_err = [field_title, err_obj.get('type', ''), err_obj.get('msg', '')]
                content.append(str.join('. ', single_err))
            self.message = str.join('\n', content)
        super().__init__(self.message)
