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

"""Unit tests for CspPath normalization and file-info caching."""

from pathlib import Path

import pyarrow.fs as arrow_fs
import pytest

from spark_rapids_tools.storagelib import CspFs, CspPath
from spark_rapids_tools.storagelib.local.localpath import LocalPath
from spark_rapids_tools.storagelib.s3.s3path import S3Path


class TestCspPathNormalization:
    """Verify normalization logic at the CspPath entry point."""

    @pytest.mark.parametrize(
        "input_path",
        [
            "s3a://bucket/path/to/file",
            "s3n://bucket/path/to/file",
            " 's3a://bucket/path/to/file' ",
        ],
    )
    def test_normalizes_s3_family_schemes(self, input_path):
        csp_path = CspPath(input_path)

        assert isinstance(csp_path, S3Path)
        assert str(csp_path) == "s3://bucket/path/to/file"
        assert csp_path.no_scheme == "bucket/path/to/file"

    @pytest.mark.parametrize("input_path", ["file:/tmp/demo.txt", "file://tmp/demo.txt", "file:////tmp/demo.txt"])
    def test_normalizes_file_scheme_variants(self, input_path):
        csp_path = CspPath(input_path)

        assert isinstance(csp_path, LocalPath)
        assert str(csp_path) == Path("/tmp/demo.txt").as_uri()
        assert csp_path.no_scheme == "/tmp/demo.txt"


class TestCspPathFileInfoCaching:
    """Verify metadata reuse and invalidation behaviour."""

    def test_list_all_reuses_file_info_from_directory_listing(self, monkeypatch, tmp_path):
        child = tmp_path / "child.txt"
        child.write_text("data", encoding="utf-8")

        root = CspPath(str(tmp_path))
        original_get_file_info = root.fs_obj.get_file_info
        direct_lookup_calls = []

        def tracking_get_file_info(path_or_selector):
            if not isinstance(path_or_selector, arrow_fs.FileSelector):
                direct_lookup_calls.append(path_or_selector)
            return original_get_file_info(path_or_selector)

        monkeypatch.setattr(root.fs_obj, "get_file_info", tracking_get_file_info)

        items = CspFs.list_all(root)
        child_path = next(item for item in items if str(item).endswith("child.txt"))

        direct_lookup_calls.clear()
        assert child_path.exists() is True
        assert child_path.base_name() == "child.txt"
        assert direct_lookup_calls == []

    def test_open_output_stream_invalidates_stale_file_info(self, tmp_path):
        created_path = CspPath(str(tmp_path / "created.txt"))

        assert created_path.exists() is False

        with created_path.open_output_stream() as output_stream:
            output_stream.write(b"payload")

        assert created_path.exists() is True
        assert created_path.is_file() is True
