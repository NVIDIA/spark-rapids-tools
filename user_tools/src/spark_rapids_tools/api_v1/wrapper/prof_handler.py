# Copyright (c) 2025, NVIDIA CORPORATION.
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

"""
Module that contains the definition of the Profiling wrapper result handler.
"""
import re
from dataclasses import dataclass

from spark_rapids_tools import override
from spark_rapids_tools.api_v1.report_reader import ToolReportReader
from spark_rapids_tools.api_v1.result_handler import register_result_class, ResultHandler


@register_result_class('profWrapperOutput')
@dataclass
class ProfWrapperResultHandler(ResultHandler):
    """
    Result handler for the profiling wrapper module. This handler is used to manage
    the final output of the profiling CLI cmd.
    """
    class Meta(ResultHandler.Meta):    # pylint: disable=too-few-public-methods
        id_regex = re.compile(r'prof_[0-9]+_[0-9a-zA-Z]+')

    @property
    def core_reader(self) -> ToolReportReader:
        return self.readers.get('profCoreOutput')

    @override
    @property
    def app_loader_reader(self) -> ToolReportReader:
        return self.core_reader
