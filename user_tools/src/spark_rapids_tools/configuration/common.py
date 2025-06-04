# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

"""Common types and definitions used by the configurations. This module is used by other
modules as well."""
import os
from pydantic import BaseModel, Field, AnyUrl, AliasChoices, ValidationError, model_validator

from spark_rapids_tools.enums import DependencyType
from spark_rapids_tools.storagelib.tools.fs_utils import FileHashAlgorithm


class BaseConfig(BaseModel, extra='forbid'):
    """
    BaseConfig class for Pydantic models that enforces the `extra = forbid`
    setting. This ensures that no extra keys are allowed in any model or
    subclass that inherits from this base class.

    This base class is meant to be inherited by other Pydantic models related
    to tools configurations so that we can enforce a global rule.
    """


class RuntimeDependencyType(BaseConfig):
    """Defines the type of runtime dependency required by the tools' java cmd."""

    dep_type: DependencyType = Field(
        description='The type of the dependency.',
        validation_alias=AliasChoices('dep_type', 'depType'))
    relative_path: str = Field(
        default=None,
        description='Specifies the relative path from within the archive file which will be added to the java cmd. '
                    'Requires field dep_type to be set to (archive).',
        validation_alias=AliasChoices('relative_path', 'relativePath'),
        examples=['jars/*'])


class DependencyVerification(BaseConfig):
    """The verification information of a runtime dependency required by the tools' java cmd."""
    size: int = Field(
        default=0,
        description='The size of the dependency file.',
        examples=[3265393])
    file_hash: FileHashAlgorithm = Field(
        default=None,
        description='The hash function to verify the file.',
        validation_alias=AliasChoices('file_hash', 'fileHash'),
        examples=[
            {
                'algorithm': 'md5',
                'value': 'bc9bf7fedde0e700b974426fbd8d869c'
            }])


class RuntimeDependency(BaseConfig):
    """Holds information about a runtime dependency required by the tools' java cmd."""
    name: str = Field(
        description='The name of the dependency.',
        examples=['Spark-3.5.0', 'AWS Java SDK'])
    uri: str = Field(
        description='The location of the dependency file. It can be a URL, a file path,'
                    'or an environment variable path (e.g., ${ENV_VAR}/file.jar).',
        examples=['file:///path/to/file.tgz',
                  'https://mvn-url/24.08.1/rapids-4-spark-tools_2.12-24.08.1.jar',
                  'gs://bucket-name/path/to/file.jar'],
        validate_default=True)
    dependency_type: RuntimeDependencyType = Field(
        default_factory=lambda: RuntimeDependencyType(dep_type=DependencyType.get_default()),
        description='Specifies the dependency type to determine how the item is processed. '
                    'For example, jar files are appended to the java classpath while archive files '
                    'such as spark are extracted first before adding subdirectory _/jars/* to the classpath.',
        validation_alias=AliasChoices('dependency_type', 'dependencyType'))
    verification: DependencyVerification = Field(
        default=None,
        description='Optional specification to verify the dependency file.')

    @model_validator(mode='after')
    def validate_dependency(self) -> 'RuntimeDependency':
        """Validate that the URI is a valid URL or file path when dependency type is not CLASSPATH."""
        dep_type = self.dependency_type.dep_type

        if dep_type == DependencyType.CLASSPATH:
            # This checks for empty strings, and whitespace-only strings.
            # None value is checked for by Pydantic validation.
            if not self.uri.strip():
                raise ValueError('URI cannot be empty for classpath dependency')
            return self

        try:
            AnyUrl(self.uri)
            return self
        except ValidationError as exc:
            if not os.path.isfile(self.uri):
                raise ValueError(f'Invalid URI for dependency: {self.uri}. Error: {exc}') from exc
            return self


class SparkProperty(BaseConfig):
    """Represents a single Spark property with a name and value."""
    name: str = Field(
        description='Name of the Spark property, e.g., "spark.executor.memory".')
    value: str = Field(
        description='Value of the Spark property, e.g., "4g".')


class SubmissionConfig(BaseConfig):
    """Base class for the tools configuration."""
