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

"""Implementation of helpers and utilities related to manage the properties and dictionaries."""

import json
from functools import partial
from json import JSONDecodeError
from pathlib import Path as PathlibPath
from typing import Union, Any, TypeVar, ClassVar, Type, Tuple, Optional

import yaml
from pyaml_env import parse_config
from pydantic import BaseModel, ConfigDict, model_validator, ValidationError

from spark_rapids_tools.exceptions import JsonLoadException, YamlLoadException, InvalidPropertiesSchema
from spark_rapids_tools.storagelib.csppath import CspPath, CspPathT
from spark_rapids_tools.utils.util import to_camel_case, to_camel_capital_case, get_elem_from_dict, get_elem_non_safe


def load_json(file_path: Union[str, CspPathT]) -> Any:
    if isinstance(file_path, str):
        file_path = CspPath(file_path)
    with file_path.open_input_stream() as fis:
        try:
            return json.load(fis)
        except JSONDecodeError as e:
            raise JsonLoadException('Incorrect format of JSON File') from e
        except TypeError as e:
            raise JsonLoadException('Incorrect Type of JSON content') from e


def load_yaml(file_path: Union[str, CspPathT]) -> Any:
    if isinstance(file_path, str):
        file_path = CspPath(file_path)
    with file_path.open_input_stream() as fis:
        try:
            return parse_config(data=fis.readall())
        except yaml.YAMLError as e:
            raise YamlLoadException('Incorrect format of Yaml File') from e


PropContainerT = TypeVar('PropContainerT', bound='AbstractPropContainer')
PropValidatorSchemaT = TypeVar('PropValidatorSchemaT', bound='PropValidatorSchema')


class PropValidatorSchema(BaseModel):
    """
    Base class that uses Pydantic to validate a given schema
    """
    model_config = ConfigDict(extra='allow')

    @classmethod
    def is_valid_schema(cls, raise_on_error: bool,
                        prop: Any) -> Tuple[bool, Optional[PropValidatorSchemaT]]:
        try:
            # Instantiate cluster_schema instance
            new_obj = cls(**prop)
            return True, new_obj
        except ValidationError as exc:
            if raise_on_error:
                raise InvalidPropertiesSchema('Invalid Schema for for the properties. ', exc) from exc
        return False, None


class PropValidatorSchemaCamel(PropValidatorSchema):
    model_config = ConfigDict(alias_generator=to_camel_case)


class PropValidatorSchemaUpper(PropValidatorSchema):
    model_config = ConfigDict(alias_generator=to_camel_capital_case)


class AbstractPropContainer(BaseModel):
    """
    An abstract class that loads properties (dictionary).
    """
    props: Any
    schema_clzz: ClassVar[Type['PropValidatorSchema']] = None

    @classmethod
    def is_valid_prop_path(cls,
                           file_path: Union[str, PathlibPath],
                           raise_on_error: bool = True) -> bool:
        return CspPath.is_file_path(file_path,
                                    extensions=['json', 'yaml', 'yml'],
                                    raise_on_error=raise_on_error)

    @model_validator(mode='before')
    @classmethod
    def validate_prop_schema(cls, data: Any) -> Any:
        if cls.schema_clzz is None:
            return data
        cls.schema_clzz.is_valid_schema(True, data.get('props'))
        return data

    @classmethod
    def load_from_file(cls,
                       file_path: Union[str, CspPathT],
                       raise_on_error: bool = True) -> Optional[PropContainerT]:
        loader_func = partial(load_json, file_path)
        if not str(file_path).endswith('.json'):
            loader_func = partial(load_yaml, file_path)
        try:
            prop = loader_func()
            new_prop_obj = cls(props=prop)
            return new_prop_obj
        except FileNotFoundError as fe:
            if raise_on_error:
                raise ValueError(f'Input file {file_path} does not exist') from fe
        except (InvalidPropertiesSchema, ValidationError) as ve:
            if raise_on_error:
                raise ve
        return None

    def get_value(self, *key_strs):
        return get_elem_from_dict(self.props, key_strs)

    def get_value_silent(self, *key_strs):
        return get_elem_non_safe(self.props, key_strs)
