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

"""Module that contains the definition of the Profiling core reader."""

from dataclasses import dataclass

from spark_rapids_tools import override
from spark_rapids_tools.api_v1.core.core_reader import CoreReaderBase
from spark_rapids_tools.api_v1.report_reader import register_report_class


@register_report_class('profCoreOutput')
@dataclass
class ProfCoreOutput(CoreReaderBase):
    """
    A class that reads the output of the Prof Core tool.
    """
    @override
    @property
    def _status_tbl(self) -> str:
        """Label for the CSV table in the report."""
        return 'profCoreCSVStatus'
