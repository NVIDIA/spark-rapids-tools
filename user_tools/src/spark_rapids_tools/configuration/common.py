# Copyright (c) 2024, NVIDIA CORPORATION.
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

from typing import Union
from pydantic import BaseModel, Field, AnyUrl, FilePath, AliasChoices

from spark_rapids_tools.enums import DependencyType
from spark_rapids_tools.storagelib.tools.fs_utils import FileHashAlgorithm


class RuntimeDependencyType(BaseModel):
    """Defines the type of dependency. It can be one of the following:
       - Archived file (.tgz)
       - Simple JAR file (*.jar)
       - Classpath directory (not yet supported)

    Note: The 'classpath' type is reserved for future use, allowing users to point to a directory
    in the classpath without needing to download or copy any binaries."""

    dep_type: DependencyType = Field(
        description='The type of the dependency',
        validation_alias=AliasChoices('dep_type', 'depType'))
    relative_path: str = Field(
        default=None,
        description='The relative path of the dependency in the classpath. This is relevant for tar files',
        validation_alias=AliasChoices('relative_path', 'relativePath'),
        examples=['jars/*'])


class DependencyVerification(BaseModel):
    """The verification information of the dependency."""
    size: int = Field(
        default=0,
        description='The size of the dependency file.',
        examples=[3265393])
    file_hash: FileHashAlgorithm = Field(
        default=None,
        description='The hash function to verify the file',
        validation_alias=AliasChoices('file_hash', 'fileHash'),
        examples=[
            {
                'algorithm': 'md5',
                'value': 'bc9bf7fedde0e700b974426fbd8d869c'
            }])


class RuntimeDependency(BaseModel):
    """The runtime dependency required by the tools Jar cmd. All elements are downloaded and added
    to the classPath."""
    name: str = Field(description='The name of the dependency')
    uri: Union[AnyUrl, FilePath] = Field(
        description='The FileURI of the dependency. It can be a local file or a remote file',
        examples=['file:///path/to/file.jar',
                  'https://mvn-url/24.08.1/rapids-4-spark-tools_2.12-24.08.1.jar',
                  'gs://bucket-name/path/to/file.jar'])
    dependency_type: RuntimeDependencyType = Field(
        default_factory=lambda: RuntimeDependencyType(dep_type=DependencyType.get_default()),
        description='The type of the dependency and how to find the lib files after decompression.',
        validation_alias=AliasChoices('dependency_type', 'dependencyType'))
    verification: DependencyVerification = Field(
        default=None,
        description='The verification information of the dependency.',
        examples=[
            {
                'size': 3265393
            },
            {
                'fileHash': {
                    'algorithm': 'md5',
                    'value': 'bc9bf7fedde0e700b974426fbd8d869c'
                }
            }
        ])
