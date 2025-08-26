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

"""Module that contains the definition of the profiling Result handler for the core module."""

import re
from dataclasses import dataclass

from spark_rapids_tools import override
from spark_rapids_tools.api_v1 import register_result_class, ResultHandler
from spark_rapids_tools.api_v1.report_reader import ToolReportReader


@register_result_class('profCoreOutput')
@dataclass
class ProfCoreResultHandler(ResultHandler):
    """Result handler for the profiling core module."""
    class Meta(ResultHandler.Meta):    # pylint: disable=too-few-public-methods
        """
        Meta class for ProfCoreResultHandler to define common attributes.
        """
        id_regex = re.compile(r'rapids_4_spark_profile')

    @override
    @property
    def alpha_reader(self) -> ToolReportReader:
        """Return the reader for the alpha report. For profCoreOutput, it is the same
        as the main report reader."""
        return self.readers.get(self.report_id)
