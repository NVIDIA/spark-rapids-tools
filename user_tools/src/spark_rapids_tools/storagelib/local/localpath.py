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

"""Wrapper implementation for local path"""

from ...utils.compat import override

from ..csppath import register_path_class, CspPath


@register_path_class('local')
class LocalPath(CspPath):
    """
    A path implementation for the local file system. This class is used to represent
    file paths on the local machine. It is a subclass of CspPath and provides
    a specific implementation for local file paths.
    """
    protocol_prefix: str = 'file://'

    @override
    def to_str_format(self) -> str:
        """
        Returns the string representation of the path in a way compatible with
        the remaining functionalities in the code. For example, modules that do not
        support remote files, then they should use this method to convert
        LocalFS to normal paths..
        This is useful for logging and debugging purposes.
        :return: The string representation of the path.
        """
        return self.no_scheme
