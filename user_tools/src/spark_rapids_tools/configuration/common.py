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
import re
from pathlib import Path
from pydantic import BaseModel, Field, AnyUrl, AliasChoices, model_validator, ValidationError, field_validator

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
        description='The location of the dependency file.It can be a URL, a file path,'
                    'or an environment variable path (e.g., ${ENV_VAR}/file.jar).',
        examples=['file:///path/to/file.tgz',
                  'https://mvn-url/24.08.1/rapids-4-spark-tools_2.12-24.08.1.jar',
                  'gs://bucket-name/path/to/file.jar',
                  '${SPARK_HOME}/jars/some.jar'])
    dependency_type: RuntimeDependencyType = Field(
        default_factory=lambda: RuntimeDependencyType(dep_type=DependencyType.get_default()),
        description='Specifies the dependency type to determine how the item is processed. '
                    'For example, jar files are appended to the java classpath while archive files '
                    'such as spark are extracted first before adding subdirectory _/jars/* to the classpath.',
        validation_alias=AliasChoices('dependency_type', 'dependencyType'))
    verification: DependencyVerification = Field(
        default=None,
        description='Optional specification to verify the dependency file.')

    @field_validator('uri')
    def validate_uri(cls, v):
        # Check for environment variable pattern
        env_var_pattern = r'^\$\{[A-Za-z_][A-Za-z0-9_]*\}.*'
        if re.match(env_var_pattern, v):
            return v

        try:
            AnyUrl(v)
            return v
        except ValidationError:
            pass

        if Path(v).exists():
            return v

        raise ValueError(
            'Dependency URI must be either:\n'
            '1. A valid environment variable path (e.g., ${ENV_VAR}/file.jar)\n'
            '2. A valid URL\n'
            '3. An existing file path'
        )

    @model_validator(mode='after')
    def validate_dependency_type_constraints(self):
        # Check if URI contains environment variable
        env_var_pattern = r'^\$\{[A-Za-z_][A-Za-z0-9_]*\}.*'
        is_env_var = re.match(env_var_pattern, self.uri) is not None

        # If URI is environment variable, dependency type cannot be archive
        if is_env_var and self.dependency_type.dep_type == DependencyType.ARCHIVE:
            raise ValueError('Archive dependency type cannot be used with environment variable URIs')

        # If dependency type is not archive, relative_path cannot be set
        if self.dependency_type.dep_type != DependencyType.ARCHIVE and self.dependency_type.relative_path:
            raise ValueError('Relative path can only be set for archive dependency types')

        return self


class SparkProperty(BaseConfig):
    """Represents a single Spark property with a name and value."""
    name: str = Field(
        description='Name of the Spark property, e.g., "spark.executor.memory".')
    value: str = Field(
        description='Value of the Spark property, e.g., "4g".')


class SubmissionConfig(BaseConfig):
    """Base class for the tools configuration."""
