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

"""Utility functions for file system operations"""

import dataclasses
import tarfile
from functools import cached_property
from typing import Optional, Union, List

from pydantic import model_validator, FilePath, AnyHttpUrl, StringConstraints, ValidationError
from pydantic.dataclasses import dataclass
from pydantic_core import PydanticCustomError
from typing_extensions import Annotated

from spark_rapids_tools import CspPath
from spark_rapids_tools.enums import HashAlgorithm
from spark_rapids_tools.storagelib import LocalPath

CspPathString = Annotated[str, StringConstraints(pattern=r'^\w+://.*')]
"""
A type alias for a string that represents a path. The path must start with a protocol scheme.
"""


def strip_extension(file_name: str, count: int = 1) -> str:
    """
    Utility method to strip the extension from a file name. By default, it only removes the last extension.
    The caller can override the count of extensions. For examples:
    :param file_name: The file name.
    :param count: The number of extensions to remove. i.e., this for files with multi-extensions.
    :return: The file name without the extension.

    Examples:
    ```py
    strip_extension('foo.tgz')                  # returns 'foo'
    strip_extension('foo.tar.gz', count = 2)    # returns 'foo'
    strip_extension('foo.tar.gz')               # returns 'foo.tar'
    ```
    """
    return file_name.rsplit('.', count)[0]


def raise_invalid_file(file_path: CspPath, msg: str, error_type: str = 'invalid_file') -> None:
    """
    Utility method to raise a custom pydantic error for invalid files. See the custom pydantic
    errors for more details: https://docs.pydantic.dev/latest/errors/errors/#custom-errors
    :param file_path: The object instantiated with the file path.
    :param msg:
    :param error_type: the error type top be displayed in the error message.
    :return:
    """
    raise PydanticCustomError(error_type, f'File {str(file_path)} {msg}')


@dataclass
class FileHashAlgorithm:
    """
    Represents a file hash algorithm and its value. Used for verification against an existing file.
    """
    algorithm: HashAlgorithm
    value: str

    """
    Example usage for the class:
    ```py
        try:
            file_algo = FileHashAlgorithm(algorithm=HashAlgorithm.SHA256, value='...')
            file_algo.verify_file(CspPath('file://path/to/file'))
        except ValidationError as e:
            print(e)
    ```
    """
    def verify_file(self, file_path: CspPath) -> bool:
        cb = self.algorithm.get_hash_func()
        with file_path.open_input_stream() as stream:
            res = cb(stream.readall())
            if res.hexdigest() != self.value:
                raise_invalid_file(file_path, f'incorrect file hash [{HashAlgorithm.tostring(self.algorithm)}]')
        return True


@dataclasses.dataclass
class FileVerificationResult:
    """
    A class that represents the result of a file verification.
    :param res_path: The path to the resource subject of verification.
    :param opts: Checking options to be passed to the FileChecker
    :param raise_on_error: Flag to raise an exception if the file is invalid.
    """
    res_path: CspPath
    opts: dict
    raise_on_error: bool = False
    validation_error: Optional[ValidationError] = dataclasses.field(default=None, init=False)

    def __post_init__(self):
        try:
            CspFileChecker(**{'file_path': str(self.res_path), **self.opts})
        except ValidationError as err:
            self.validation_error = err
            if self.raise_on_error:
                raise err

    @cached_property
    def successful(self) -> bool:
        return self.validation_error is None


@dataclass
class CspFileChecker:
    """
    A class that represents a file checker. It is used as a pydantic model to validate the file.
    :param file_path: The path to the file. It accepts any valid CspPath or AnyHttpUrl in case we want to
           verify an url path extension even before we download it.
    :param must_exist: When True, the file must exist.
    :param is_file: When True, the file must be a file. Otherwise, it is a directory.
    :param size: The expected size of the file. When 0, it is not checked.
    :param extensions: A list of expected extensions. When None, it is not checked.
    :param file_hash: A hash algorithm and its value to verify the file.
    ```py
    try:
        TypeAdapter(CspFileChecker).validate_python({
            'file_path': 'file:///var/tmp/spark_cache_folder_test/rapids-4-spark-tools_2.12-24.08.2.jar',
            'must_exist': True,
            'size': .....,
            'extensions': ['jar']})
    except ValidationError as e:
        print(e)
    ```
    """
    file_path: Union[CspPathString, FilePath, AnyHttpUrl]
    must_exist: Optional[bool] = False
    is_file: Optional[bool] = True
    size: Optional[int] = 0
    extensions: Optional[List[str]] = None
    file_hash: Optional[FileHashAlgorithm] = None
    # TODO add verification for the modified time.

    @cached_property
    def csp_path(self) -> CspPath:
        return CspPath(str(self.file_path))

    @model_validator(mode='after')
    def verify_file(self) -> 'CspFileChecker':
        if self.must_exist and not self.csp_path.exists():
            raise_invalid_file(self.file_path, 'does not exist')
        if self.is_file and not self.csp_path.is_file():
            raise_invalid_file(self.file_path, 'expected a file but got a directory')
        if self.size and self.size > 0:
            if self.csp_path.size != self.size:
                raise_invalid_file(self.file_path, f'size {self.csp_path.size} does not have the expected size')
        if self.extensions:
            file_ext = self.csp_path.extension_from_path()
            if not any(file_ext == ext for ext in self.extensions):
                raise_invalid_file(self.file_path,
                                   f'[{file_ext}] does not match the expected extensions')
        if self.file_hash:
            self.file_hash.verify_file(self.csp_path)
        return self


def untar_file(file_path: LocalPath, dest_folder: LocalPath) -> CspPath:
    """
    Utility method to decompress a tgz file.
    :param file_path: The path to the tar file.
    :param dest_folder: The destination folder to untar the file.
    """
    dest_folder.create_dirs()
    # the compressed file must be local and must exist
    FileVerificationResult(res_path=file_path, opts={'must_exist': True, 'is_file': True}, raise_on_error=True)
    with tarfile.open(file_path.no_scheme, mode='r:*') as tar:
        tar.extractall(dest_folder.no_scheme)
        tar.close()
    extracted_name = strip_extension(file_path.base_name())
    result = dest_folder.create_sub_path(extracted_name)
    # we do not need to verify that it is a folder because the result might be a file.
    return result
