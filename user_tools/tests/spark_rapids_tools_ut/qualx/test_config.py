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

"""Test qualx_config module"""
import pytest  # pylint: disable=import-error
from spark_rapids_tools.tools.qualx.config import (
    get_cache_dir,
    get_label,
)
from ..conftest import SparkRapidsToolsUT


class TestConfig(SparkRapidsToolsUT):
    """Test class for qualx_config module"""
    def test_get_cache_dir(self, monkeypatch):
        # Test with mock environment variable
        monkeypatch.setenv('QUALX_CACHE_DIR', 'test_cache')
        assert get_cache_dir() == 'test_cache'

        # Test without environment variable (should use default)
        monkeypatch.delenv('QUALX_CACHE_DIR')
        assert get_cache_dir() == 'qualx_cache'

    def test_get_label(self, monkeypatch):
        # Test with duration_sum
        monkeypatch.setenv('QUALX_LABEL', 'duration_sum')
        assert get_label() == 'duration_sum'

        # Test with unsupported label
        with pytest.raises(AssertionError):
            monkeypatch.setenv('QUALX_LABEL', 'duration')
            get_label()

        # Test without environment variable (should use default)
        monkeypatch.delenv('QUALX_LABEL')
        assert get_label() == 'Duration'
