# Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

"""Build helpers."""

import datetime
import os


def get_version(main=None):
    if main is None:
        # pylint: disable=import-outside-toplevel
        from spark_rapids_pytools import VERSION as main
    suffix = ''
    nightly = os.environ.get('USERTOOLS_NIGHTLY')
    if nightly == '1':
        suffix = '.dev' + datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    return main + suffix
