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

"""init file of the api_v1 package which offers a common interface to consume the tools
output."""

from .common import (
    APIUtils,
    LoadCombinedRepResult
)
from .app_handler import (
    AppHandler
)
from .report_reader import (
    ToolReportReaderT
)
from .result_handler import (
    register_result_class,
    result_registry,
    ResultHandler,
    ToolResultHandlerT
)
from .wrapper import (
    QualWrapperResultHandler,
    ProfWrapperResultHandler
)
from .core import (
    QualCoreResultHandler,
    ProfCoreResultHandler
)
from .builder import (
    CSVReportCombiner,
    CSVReport,
    QualCore,
    ProfCore,
    QualWrapper,
    ProfWrapper,
    APIResHandler,
    CombinedCSVBuilder
)

__all__ = [
    'APIUtils',
    'LoadCombinedRepResult',
    'AppHandler',
    'ToolReportReaderT',
    'register_result_class',
    'result_registry',
    'ResultHandler',
    'ToolResultHandlerT',
    'QualWrapperResultHandler',
    'QualCoreResultHandler',
    'ProfWrapperResultHandler',
    'ProfCoreResultHandler',
    'CSVReportCombiner',
    'CSVReport',
    'QualCore',
    'ProfCore',
    'QualWrapper',
    'ProfWrapper',
    'APIResHandler',
    'CombinedCSVBuilder'
]
