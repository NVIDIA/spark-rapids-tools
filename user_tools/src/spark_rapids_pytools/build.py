# Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import os


def get_version(main: str = None) -> str:
    if main is None:
        # pylint: disable=import-outside-toplevel
        from spark_rapids_pytools import VERSION as main
    suffix = ''
    nightly = os.environ.get('USERTOOLS_NIGHTLY')
    if nightly == '1':
        build_number = os.environ.get('BUILD_NUMBER', '0')
        suffix = f'.dev{build_number}'
    return main + suffix


def get_spark_dep_version(spark_dep_arg: str = None) -> str:
    """
    Get the runtime SPARK build_version for the user tools environment.
    Note that the env_var always have precedence over the input argument and the default values
    :param spark_dep_arg: optional argument to specify the build version
    :return: the first value set in the following order:
       1- env_var RAPIDS_USER_TOOLS_SPARK_DEP_VERSION
       2- the input buildver_arg
       3- default value SPARK_DEV_VERSION
    """
    if spark_dep_arg is None:
        # pylint: disable=import-outside-toplevel
        from spark_rapids_pytools import SPARK_DEP_VERSION
        spark_dep_arg = SPARK_DEP_VERSION
    # the env_var should have precedence because this is the way user can override the default configs
    return os.environ.get('RAPIDS_USER_TOOLS_SPARK_DEP_VERSION', spark_dep_arg)
