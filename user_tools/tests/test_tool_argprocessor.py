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

"""Test Tool argument validators"""


import fire
import pytest

from as_pytools.cmdli.argprocessor import AbsToolUserArgModel


class TestToolArgProcessor:  # pylint: disable=too-few-public-methods
    """
    Class testing toolArgProcessor functionalities
    """

    @pytest.mark.parametrize('tool_name', ['qualification', 'profiling', 'bootstrap'])
    def test_no_args(self, tool_name):
        fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            AbsToolUserArgModel.create_tool_args(tool_name)
        assert pytest_wrapped_e.type == SystemExit
