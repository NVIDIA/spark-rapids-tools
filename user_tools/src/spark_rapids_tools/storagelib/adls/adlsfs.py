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

"""Wrapper for the ADLS File system"""

from typing import Any

import adlfs
from pyarrow.fs import PyFileSystem, FSSpecHandler

from ..cspfs import CspFs, BoundedArrowFsT, register_fs_class


@register_fs_class("adls", "PyFileSystem")
class AdlsFs(CspFs):
    """Access Azure Datalake Gen2 and Azure Storage if it were a file system using Multiprotocol
    Access (Docstring copied from adlfs).

    Since AzureBlobFileSystem follows the fsspec interface, this class wraps it into a python-based
    PyArrow filesystem (PyFileSystem) using FSSpecHandler.

    The initialization of the filesystem looks for the following env_variables:
    AZURE_STORAGE_ACCOUNT_NAME
    AZURE_STORAGE_ACCOUNT_KEY
    AZURE_STORAGE_CONNECTION_STRING
    AZURE_STORAGE_SAS_TOKEN
    AZURE_STORAGE_CLIENT_ID
    AZURE_STORAGE_CLIENT_SECRET
    AZURE_STORAGE_TENANT_ID
    """

    @classmethod
    def create_fs_handler(cls, *args: Any, **kwargs: Any) -> BoundedArrowFsT:
        azure_fs = adlfs.AzureBlobFileSystem(*args, **kwargs)
        return PyFileSystem(FSSpecHandler(azure_fs))
