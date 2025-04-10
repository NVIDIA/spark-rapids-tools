# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

"""Implementation of storage related functionalities."""

import datetime
import glob
import hashlib
import os
import pathlib
import re
import shutil
import ssl
import subprocess
import urllib
from dataclasses import dataclass
from functools import partial
from itertools import islice
from shutil import rmtree
from typing import List

import certifi
from fastcore.all import urlsave
from fastprogress.fastprogress import progress_bar

from spark_rapids_pytools.common.exceptions import StorageException
from spark_rapids_pytools.common.utilities import Utils, SysCmd


class FSUtil:
    """Implementation of storage functionality for local disk."""

    @classmethod
    def remove_ext(cls, file_path) -> str:
        return os.path.splitext(file_path)[0]

    @classmethod
    def get_all_files(cls, curr_path) -> list:
        return glob.glob(f'{curr_path}/*', recursive=False)

    @classmethod
    def get_abs_path(cls, curr_path) -> str:
        return os.path.abspath(curr_path)

    @classmethod
    def get_resource_name(cls, full_path: str) -> str:
        url_parts = full_path.split('/')[-1:]
        return url_parts[0]

    @classmethod
    def build_full_path(cls, parent_path, item) -> str:
        full_path = os.path.abspath(parent_path)
        return os.path.join(full_path, item)

    @classmethod
    def build_path(cls, path, item) -> str:
        return os.path.join(path, item)

    @classmethod
    def build_url_from_parts(cls, *parts) -> str:
        url_parts = [part.strip('/') for part in parts[:-1]]
        # we do not want to remove the rightmost slash if any
        url_parts.append(parts[-1].lstrip('/'))
        return Utils.gen_joined_str('/', url_parts)

    @classmethod
    def remove_path(cls, file_path: str, fail_ok: bool = False):
        try:
            if os.path.isdir(file_path):
                rmtree(file_path, ignore_errors=fail_ok)
            else:
                os.remove(file_path)
        except OSError as err:
            if not fail_ok:
                raise StorageException(f'Could not remove directory {file_path}') from err

    @classmethod
    def make_dirs(cls, dir_path: str, exist_ok: bool = True):
        try:
            os.makedirs(dir_path, exist_ok=exist_ok)
        except OSError as err:
            raise StorageException(f'Error Creating directories {dir_path}') from err

    @classmethod
    def copy_resource(cls, src: str, dest: str) -> str:
        abs_src = os.path.abspath(src)
        abs_dest = os.path.abspath(dest)
        # check if path exists
        if not os.path.exists(abs_src):
            raise StorageException('Error copying resource on local disk. '
                                   f'Resource {abs_src} does not exist')

        # Determine whether to copy a single resource or a directory
        if os.path.isfile(abs_src):
            return shutil.copy2(abs_src, abs_dest)
        return shutil.copytree(abs_src, abs_dest, dirs_exist_ok=True)

    @classmethod
    def cache_resource(cls, src: str, dest: str):
        abs_src = os.path.abspath(src)
        abs_dest = os.path.abspath(dest)
        # check if path exists
        if not os.path.exists(abs_src):
            raise StorageException('Error copying resource on local disk. '
                                   f'Resource {abs_src} does not exist')
        with open(abs_src, 'rb') as s:
            with open(abs_dest, 'wb') as d:
                shutil.copyfileobj(s, d)

    @classmethod
    def download_from_url(cls,
                          src_url: str,
                          dest: str) -> str:
        resource_name = cls.get_resource_name(src_url)
        # We create a context here to fix and issue with urlib requests issue
        dest_file = cls.build_path(dest, resource_name)
        context = ssl.create_default_context(cafile=certifi.where())
        with urllib.request.urlopen(src_url, context=context) as resp:
            with open(dest_file, 'wb') as f:
                shutil.copyfileobj(resp, f)
        return dest_file

    @classmethod
    def fast_download_url(cls, url: str, fpath: str, timeout=None, pbar_enabled=False) -> str:
        """
        Download the given url and display a progress bar
        """
        pbar = progress_bar([])

        def progress_bar_cb(count=1, bsize=1, total_size=None):
            pbar.total = total_size
            pbar.update(count * bsize)

        return urlsave(url, fpath, reporthook=progress_bar_cb if pbar_enabled else None, timeout=timeout)

    @classmethod
    def verify_file(cls, fpath: str, file_checks: dict) -> bool:
        if not os.path.exists(fpath):
            return False
        if not file_checks:
            return True
        expiration_time_s = file_checks.get('cacheExpirationSecs')
        if expiration_time_s:
            modified_time = os.path.getmtime(fpath)
            diff_time = int(datetime.datetime.now().timestamp() - modified_time)
            if diff_time > expiration_time_s:
                return False
        return FileVerifier.check_integrity(fpath, file_checks)

    @classmethod
    def cache_from_url(cls,
                       src_url: str,
                       cache_file: str,
                       file_checks: dict = None) -> bool:
        """
        download a resource from given URL as a destination cache_file
        :param src_url: HTTP url containing the resource
        :param cache_file: the file where the resource is saved. It is assumed that this the file
        :param file_checks: a dictionary that contains the criteria to check that the file is the
               same.
        :return: true if the file is re-downloaded. False, if the cached file is not modified.
        """
        curr_time_stamp = datetime.datetime.now().timestamp()
        if cls.verify_file(cache_file, file_checks):
            # the file already exists and matches the validation
            # update the access-time and return True
            # update modified time and access time
            return False
        # download the file
        cls.fast_download_url(src_url, cache_file)
        # update modified time and access time
        os.utime(cache_file, times=(curr_time_stamp, curr_time_stamp))
        if not cls.verify_file(cache_file, file_checks):
            raise RuntimeError(f'Failed downloading resource {src_url}')
        return True

    @classmethod
    def get_home_directory(cls) -> str:
        return os.path.expanduser('~')

    @classmethod
    def expand_path(cls, file_path) -> str:
        """
        Used to expand the files with path starting with home directory. this is used because some
        libraries like configparser does not expand correctly
        :param file_path: the file path
        :return: the expanded file path
        """
        if file_path.startswith('~'):
            new_path = pathlib.PosixPath(file_path)
            return str(new_path.expanduser())
        return file_path

    @classmethod
    def get_subdirectories(cls, dir_path) -> list:
        """
        Given a directory, list all the subdirectories without recursion
        :param dir_path: the directory parent that we are interested in
        :return: a list of subdirectories with full path
        """
        res = []
        subfolders = glob.glob(f'{dir_path}/*', recursive=False)
        for subfolder in subfolders:
            if os.path.isdir(subfolder):
                res.append(subfolder)
        return res

    @classmethod
    def gen_dir_tree(cls,
                     dir_path: pathlib.Path,
                     depth_limit: int = -1,
                     population_limit: int = 1024,
                     limit_to_directories: bool = False,
                     exec_dirs: List[str] = None,
                     exec_files: List[str] = None,
                     indent=''):
        # the implementation is based on the answer posted on stackoverflow
        # https://stackoverflow.com/a/59109706
        dir_patterns = [re.compile(rf'{p}') for p in exec_dirs] if exec_dirs else []
        file_patterns = [re.compile(rf'{p}') for p in exec_files] if exec_files else []
        res_arr = []
        dir_path = pathlib.Path(dir_path)
        token_ws = '    '
        token_child = '│   '
        token_sibling = '├── '
        token_leaf = '└── '
        files_count = 0
        dir_count = 0

        def inner(dir_p: pathlib.Path, prefix: str = '', level=-1):
            nonlocal files_count, dir_count
            if not level:
                return  # 0, stop iterating
            sub_items = []
            for f in dir_p.iterdir():
                if f.is_dir():
                    is_excluded = any(p.match(f.name) for p in dir_patterns)
                else:
                    is_excluded = limit_to_directories
                    if not is_excluded:
                        is_excluded = any(p.match(f.name) for p in file_patterns)
                if not is_excluded:
                    sub_items.append(f)

            pointers = [token_sibling] * (len(sub_items) - 1) + [token_leaf]
            for pointer, path in zip(pointers, sub_items):
                if path.is_dir():
                    yield prefix + pointer + path.name
                    dir_count += 1
                    extension = token_child if pointer == token_sibling else token_ws
                    yield from inner(path, prefix=prefix + extension, level=level - 1)
                elif not limit_to_directories:
                    yield prefix + pointer + path.name
                    files_count += 1

        res_arr.append(f'{indent}{dir_path.name}')
        iterator = inner(dir_path, level=depth_limit)
        for line in islice(iterator, population_limit):
            res_arr.append(f'{indent}{line}')
        if next(iterator, None):
            res_arr.append(f'{indent}... length_limit, {population_limit}, reached, counted:')
        res_arr.append(f'{indent}{dir_count} directories'
                       f', {files_count} files' if files_count else '')
        return res_arr

    @classmethod
    def resource_exists(cls, src) -> bool:
        return os.path.exists(src)


@dataclass
class StorageDriver:
    """
    Wrapper to interface with archiving command, such as copying/moving/listing files.
    """

    def resource_exists(self, src) -> bool:
        return os.path.exists(src)

    def resource_is_dir(self, src) -> bool:
        return os.path.isdir(src)

    def _download_remote_resource(self, src: str, dest: str) -> str:
        """
        given a path or url file, downloads the resource to local disk.
        Note that the dest needs to be absolute path. src can be either folder or a single file
        :param src: url/local path of the resource
        :param dest: directory folder where the resource is downloaded
        :return: the full path of the target
        """
        if src.startswith('http'):
            # this is url resource
            return FSUtil.download_from_url(src, dest)
        # this is a folder-to-folder download
        return FSUtil.copy_resource(src, dest)

    def download_resource(self,
                          src: str,
                          dest: str,
                          fail_ok: bool = False,
                          create_dir: bool = True) -> str:
        """
        Copy a resource from remote storage or from external local storage into the dest directory
        :param src: the path/url of the resource to be copied. It can be a single file or a directory
        :param dest: the directory where the resource is being copied
        :param fail_ok: whether to raise an exception on failure
        :param create_dir: create the directories of the destination if they do not exist
        :return: full path of the destination resource dest/resource_name
        """
        try:
            abs_dest = FSUtil.get_abs_path(dest)
            if create_dir:
                FSUtil.make_dirs(abs_dest)
            return self._download_remote_resource(src, abs_dest)
        except StorageException as store_ex:
            if not fail_ok:
                raise store_ex
            return None

    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None):
        del exclude_pattern
        return FSUtil.copy_resource(src, dest)

    def upload_resource(self,
                        src: str,
                        dest: str,
                        fail_ok: bool = False,
                        exclude_pattern: str = None) -> str:
        try:
            abs_src = FSUtil.get_abs_path(src)
            if not self.resource_exists(abs_src):
                raise StorageException(f'Resource {abs_src} cannot be copied to {dest}. '
                                       f'{abs_src} does not exist')
            return self._upload_remote_dest(abs_src, dest, exclude_pattern=exclude_pattern)
        except StorageException as store_ex:
            if not fail_ok:
                raise store_ex
            return None

    def _delete_path(self, src, fail_ok: bool = False):
        FSUtil.remove_path(src, fail_ok=fail_ok)

    def remove_resource(self,
                        src: str,
                        fail_ok: bool = False):
        """
        Given a path delete it permanently and all its contents recursively
        :param src: the path of the resource to be removed
        :param fail_ok: raise exception
        :return:
        """
        try:
            self._delete_path(src, fail_ok=fail_ok)
        except StorageException as store_ex:
            if not fail_ok:
                raise store_ex

    def is_file_path(self, value: str):
        """
        given a string value, check whether this is a valid file path or url
        :param value: the string to be evaluated
        :return: True if formatting wise, it matches file path
        """
        if value is None:
            return False
        if value.startswith('http'):
            return True
        if '/' in value:
            # slash means this is a file
            return True
        # check if the file ends with common extension
        return value.endswith('.json') or value.endswith('.yaml') or value.endswith('.yml')


class FileVerifier:
    """
    Utility class to verify the integrity of a downloaded file using hash algorithms.
    Supported hash algorithms: md5, sha1, sha256, sha512.
    """
    SUPPORTED_ALGORITHMS = {
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
        'sha256': hashlib.sha256,
        'sha512': hashlib.sha512
    }
    GPG_TIMEOUT_SEC = 60    # Timeout for GPG process
    READ_CHUNK_SIZE = 8192  # Size of chunk in bytes
    GPG_SIGNATURE_ENABLED = False  # enable/disable gpg-signature usage

    @classmethod
    def get_signature_file(cls, file_url: str, dest_folder: str):
        try:
            return FSUtil.download_from_url(file_url + '.asc', dest_folder)
        except urllib.error.URLError:
            return None

    @classmethod
    def get_integrity_algorithm(cls, hash_info: dict):
        for algorithm in cls.SUPPORTED_ALGORITHMS:
            if algorithm in hash_info:
                return algorithm
        return None

    @classmethod
    def _gpg_prerequisites_satisfied(cls) -> bool:
        return cls.GPG_SIGNATURE_ENABLED and Utils.is_system_tool('gpg')

    @classmethod
    def _check_integrity_using_gpg(cls, file_path: str, signature_file_path: str) -> bool:
        """
        Verify file integrity using GPG and its corresponding .asc signature file.
        Note - The verification has a timeout of `GPG_TIMEOUT_SEC`

        :param file_path: Path to the file to be verified.
        :param signature_file_path: Path to the .asc signature file.
        :return: True if the file integrity is valid and the signature is verified, False otherwise.
        """
        if not (os.path.isfile(file_path) and os.path.isfile(signature_file_path)):
            return False

        assert cls._gpg_prerequisites_satisfied()
        gpg_command = [
            'gpg',
            '--auto-key-locate keyserver',
            '--keyserver pgp.mit.edu',
            '--keyserver-options auto-key-retrieve',
            '--verify',
            signature_file_path,
            file_path
        ]
        gpg_cmd_args = {
            'cmd': gpg_command,
            'timeout_secs': cls.GPG_TIMEOUT_SEC
        }
        try:
            gpg_cmd_obj = SysCmd().build(gpg_cmd_args)
            result = gpg_cmd_obj.exec()
            return 'Good signature' in result
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            return False

    @classmethod
    def _check_integrity_using_algorithm(cls, file_path: str, algorithm: str, expected_hash: str) -> bool:
        """
        Checks the integrity of a downloaded file by calculating its hash and comparing it with the expected hash.

        :param file_path: Path of the downloaded file.
        :param algorithm: Name of the hash algorithm to use for calculating
        :param expected_hash: Expected hash value for the file
        :return: True if the calculated hash matches the expected hash, False otherwise.
        """
        if not os.path.isfile(file_path):
            # Cannot verify file
            return False

        # Helper function to calculate the hash of the file using the specified algorithm
        def calculate_hash(hash_algorithm):
            hash_function = cls.SUPPORTED_ALGORITHMS[hash_algorithm]()
            with open(file_path, 'rb') as file:
                while chunk := file.read(cls.READ_CHUNK_SIZE):
                    hash_function.update(chunk)
            return hash_function.hexdigest()

        calculated_hash = calculate_hash(algorithm)
        return calculated_hash == expected_hash

    @classmethod
    def check_integrity(cls, file_path: str, check_args: dict) -> bool:
        """
        Check the integrity of a downloaded file.This method checks the integrity of a downloaded file using
        hash algorithms or GPG verification, if available.

        :param file_path: Path of the downloaded file.
        :param check_args: Dictionary containing the hash algorithm, expected hash value and/or the signature file path.
        :return:
        """
        # shortcircuit. if size is available. verify the size if correct.
        expected_size = check_args.get('size')
        if expected_size:
            if expected_size != os.path.getsize(file_path):
                return False
        # Return True if no verification can be performed.
        # Otherwise, the verification would fail on every single time a file is downloaded;
        # especially if the gpg is not installed
        result = True
        cb = None

        if 'signatureFile' in check_args and cls._gpg_prerequisites_satisfied():
            # try to use gpg if available
            signature_file_path = check_args.get('signatureFile')
            if signature_file_path is not None:
                cb = partial(cls._check_integrity_using_gpg, file_path, signature_file_path)
        if cb is None:
            # if cb is still None, Verify integrity using hashing algorithm
            hashlib_args = check_args.get('hashlib')
            if hashlib_args:
                algo = hashlib_args['algorithm']
                hash_value = hashlib_args['hash']
                cb = partial(cls._check_integrity_using_algorithm, file_path, algo, hash_value)
        if cb is not None:
            # the call back is set, then we can run the verification
            result = cb()
        return result
