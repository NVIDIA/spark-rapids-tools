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

"""Abstract class for FS that wraps on top of the remote clients"""

import abc
import os
from typing import Generic, Callable, TypeVar, Any, Union, List, Optional

from pyarrow import fs as arrow_fs
from pyarrow.fs import FileType

from .csppath import CspPathImplementation, CspPath, path_impl_registry

from ..exceptions import (
    CspPathNotFoundException, CspPathTypeMismatchError
)

BoundedCspPath = TypeVar('BoundedCspPath', bound=CspPath)
BoundedArrowFsT = TypeVar('BoundedArrowFsT', bound=arrow_fs.FileSystem)


def register_fs_class(key: str, fs_subclass: str) -> Callable:
    def decorator(cls: type) -> type:
        if not issubclass(cls, CspFs):
            raise TypeError('Only subclasses of CspFs can be registered.')
        path_impl_registry[key].fs_class = cls
        path_impl_registry[key].name = key
        imported_module = __import__('pyarrow.fs', globals(), locals(), [fs_subclass])
        defined_clzz = getattr(imported_module, fs_subclass)
        path_impl_registry[key].fslib_class = defined_clzz
        cls._path_meta = path_impl_registry[key]  # pylint: disable=protected-access
        return cls

    return decorator


def custom_dir(orig_type, new_type):
    """
    Given a type orig_type, it adds the attributes found in the new_type. used by the delegator.
    See the description in CspFs class.
    """
    return dir(type(orig_type)) + list(orig_type.__dict__.keys()) + new_type


class CspFs(abc.ABC, Generic[BoundedCspPath]):
    """
    Abstract FileSystem that provides input and output streams as well as directory operations.

    The datapaths are abstract representations.
    This class uses delegations to utilize the interface implemented in the pyArrow Filesystem
    class. That way, we don't have to rewrite every single API.
    Instead, if the attribute exists in the child class it is going to be used.
    See more explanation in the blogpost
    https://www.fast.ai/posts/2019-08-06-delegation.html
    Base class for attr accesses in "self._xtra" passed down to "self.fs"
    """
    _path_meta: CspPathImplementation
    _default_fs = None

    @classmethod
    def create_fs_handler(cls, *args: Any, **kwargs: Any) -> BoundedArrowFsT:
        return cls._path_meta.fslib_class(*args, **kwargs)

    @classmethod
    def get_default_client(cls) -> 'CspFs':
        if cls._default_fs is None:
            cls._default_fs = cls()
        return cls._default_fs

    @property
    def _xtra(self):
        """returns the members defined in the child class as long as they are not protected"""
        return [o for o in dir(self.fs) if not o.startswith('_')]

    def __getattr__(self, k):
        """returns the members defined in the child class as long as they are not protected"""
        if k in self._xtra:
            return getattr(self.fs, k)
        raise AttributeError(k)

    def __dir__(self):
        """extends the list of attributes to include the child class"""
        return custom_dir(self, self._xtra)

    def __init__(self, *args: Any, **kwargs: Any):
        self.fs = self.create_fs_handler(*args, **kwargs)

    def create_as_path(self, entry_path: Union[str, BoundedCspPath]) -> BoundedCspPath:
        return self._path_meta.path_class(entry_path=entry_path, fs_obj=self)

    @classmethod
    def copy_file(cls, src: BoundedCspPath, dest: BoundedCspPath):
        """
        Copy a single file between FileSystems. This function assumes that
        :param src:
        :param dest:
        :return:
        """
        arrow_fs.copy_files(src.no_scheme, dest.no_scheme,
                            source_filesystem=src.fs_obj.fs,
                            destination_filesystem=dest.fs_obj.fs,
                            # 64 MB chunk size
                            chunk_size=64 * 1024 * 1024)

    @classmethod
    def copy_resources(cls, src: BoundedCspPath, dest: BoundedCspPath):
        """
        Copy files between FileSystems.

        This functions allows you to recursively copy directories of files from
        one file system to another, such as from S3 to your local machine. Note that the
        copy_resources uses threads by default. The chunk size is set to 1 MB.

        :param src: BoundedCspPath
            Source file path or URI to a single file or directory
            If a directory, files will be copied recursively from this path.
        :param dest: BoundedCspPath
            Destination directory where the source is copied to.
            If the directory does not exist, it will be created first.
            If the source is a file, then the final destination will be dest/file_name
            If the source is a directory, then a new folder is created under dest as
            "dest/src".
        """
        # check that the src path exists
        if not src.exists():
            raise CspPathNotFoundException(f'Source Path does not exist {src}')
        dest_path = os.path.join(str(dest), src.base_name())
        if src.is_dir():
            # create a subfolder in the destination
            dest_path = os.path.join(str(dest), src.base_name())
            dest = dest.fs_obj.create_as_path(entry_path=dest_path)
            # dest must be a directory. make sure it exists
            dest.create_dirs()
        else:
            dest.create_dirs()
            dest = dest.fs_obj.create_as_path(entry_path=dest_path)

        arrow_fs.copy_files(src.no_scheme, dest.no_scheme,
                            source_filesystem=src.fs_obj.fs,
                            destination_filesystem=dest.fs_obj.fs,
                            # 64 MB chunk size
                            chunk_size=64 * 1024 * 1024)

    @classmethod
    def list_all_files(cls, path: BoundedCspPath) -> List[BoundedCspPath]:
        return cls._list_items_by_type(path, FileType.File)

    @classmethod
    def list_all_dirs(cls, path: BoundedCspPath) -> List[BoundedCspPath]:
        return cls._list_items_by_type(path, FileType.Directory)

    @classmethod
    def list_all(cls, path: BoundedCspPath) -> List[BoundedCspPath]:
        return cls._list_items_by_type(path, None)

    @staticmethod
    def _list_items_by_type(path: BoundedCspPath, item_type: Optional[FileType]) -> List[BoundedCspPath]:
        """
        Helper function to list files, directories, or all items in the given path.
        """
        if not path.exists():
            raise CspPathNotFoundException(f'Path does not exist: {path}')
        if not path.is_dir():
            raise CspPathTypeMismatchError(f'Path is not a directory: {path}')

        dir_info_list = path.fs_obj.get_file_info(arrow_fs.FileSelector(path.no_scheme, recursive=False))
        return [
            path.create_sub_path(dir_info.base_name)
            for dir_info in dir_info_list
            if item_type is None or dir_info.type == item_type
        ]
