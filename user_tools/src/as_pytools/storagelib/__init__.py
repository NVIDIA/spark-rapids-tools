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

"""init file of the storagelib package which offers a common interface to access any FS protocol."""

from .s3.s3fs import S3Fs
from .s3.s3path import S3AsPath
from .gs.gcsfs import GcFs
from .gs.gcpath import GSAsPath
from .hdfs.hdfsfs import HdfsFs
from .hdfs.hdfspath import HdfsPath
from .adls.adlsfs import AdlsFs
from .adls.adlspath import AdlsAsPath
from .local.localfs import LocalFs
from .local.localpath import LocalAsPath
from .aspath import ASFsPathT, path_impl_registry, ASFsPath
from .fs import AsFs, BoundedArrowFsT, register_fs_class

__all__ = [
    'AdlsFs',
    'AdlsAsPath',
    'AsFs',
    'ASFsPath',
    'BoundedArrowFsT',
    'GcFs',
    'GSAsPath',
    'HdfsFs',
    'HdfsPath',
    'LocalFs',
    'LocalAsPath',
    'ASFsPath',
    'ASFsPathT',
    'path_impl_registry',
    'register_fs_class',
    'S3Fs',
    'S3AsPath',
]
