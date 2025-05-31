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

import time
import pytest
from unittest.mock import patch
from spark_rapids_tools.utils.net_utils import download_url_request


@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file for testing."""
    return str(tmp_path / "test_download.txt")


class MockResponse:
    def __init__(self, chunks):
        self._chunks = chunks

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size=None):
        return iter(self._chunks)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_response():
    """Create a mock response with chunks of data."""
    chunks = [b'chunk1', b'chunk2', b'chunk3', b'chunk4']
    return MockResponse(chunks)


class SlowMockResponse(MockResponse):
    def iter_content(self, chunk_size=None):
        def slow_iter():
            for chunk in self._chunks:
                time.sleep(0.1)
                yield chunk
        return slow_iter()


@pytest.fixture
def slow_mock_response():
    """Create a mock response that simulates slow download."""
    chunks = [b'chunk1', b'chunk2', b'chunk3', b'chunk4']
    return SlowMockResponse(chunks)


def test_normal_download(temp_file, mock_response):
    """Test normal download without timeout."""
    with patch('requests.get', return_value=mock_response):
        result = download_url_request('http://test.com/file', temp_file, timeout=10)
        assert result == temp_file
        with open(temp_file, 'rb') as f:
            content = f.read()
        assert content == b'chunk1chunk2chunk3chunk4'


def test_timeout_before_last_chunk(temp_file, slow_mock_response):
    """Test timeout before the last chunk."""
    with patch('requests.get', return_value=slow_mock_response):
        with pytest.raises(TimeoutError) as exc_info:
            download_url_request('http://test.com/file', temp_file, timeout=0.2)
        assert "Download timed out after 0.2 seconds" in str(exc_info.value)


def test_timeout_on_last_chunk(temp_file, slow_mock_response):
    """Test timeout on the last chunk - should complete successfully."""
    with patch('requests.get', return_value=slow_mock_response):
        result = download_url_request('http://test.com/file', temp_file, timeout=0.35)
        assert result == temp_file
        with open(temp_file, 'rb') as f:
            content = f.read()
        assert content == b'chunk1chunk2chunk3chunk4'
