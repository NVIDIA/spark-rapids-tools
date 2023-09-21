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

"""Wrapper for the Google storage File system"""

from ..cspfs import CspFs, register_fs_class


@register_fs_class('gcs', 'GcsFileSystem')
class GcsFs(CspFs):
    """Implementation of FileSystem for Google storage on top of pyArrow
    (Docstring copied from pyArrow.GcsFileSystem).

    The GcsFileSystem is initialized with the following list of arguments:

    >>> GcsFileSystem(bool anonymous=False, *,
    ...    access_token=None, target_service_account=None,
    ...    credential_token_expiration=None, default_bucket_location='US',
    ...    scheme=None, endpoint_override=None, default_metadata=None, retry_time_limit=None)

    the constructor uses the process described in https://google.aip.dev/auth/4110
    to resolve credentials. If not running on Google Cloud Platform (GCP), this generally requires
    the environment variable GOOGLE_APPLICATION_CREDENTIALS to point to a JSON file containing
    credentials.
    """
