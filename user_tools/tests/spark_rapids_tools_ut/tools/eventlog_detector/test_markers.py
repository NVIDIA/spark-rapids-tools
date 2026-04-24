# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Unit tests for ``eventlog_detector.markers``."""
# pylint: disable=too-few-public-methods  # test classes naturally have few methods

from spark_rapids_tools.tools.eventlog_detector import markers as m


class TestGpuMarkers:
    """Test RAPIDS plugin marker constants."""

    def test_plugin_substring_exact(self):
        assert m.GPU_PLUGIN_CLASS_SUBSTRING == "com.nvidia.spark.SQLPlugin"

    def test_gpu_toggle_key_exact(self):
        assert m.GPU_ENABLED_KEY == "spark.rapids.sql.enabled"

    def test_build_info_event_exact(self):
        assert m.EVENT_SPARK_RAPIDS_BUILD_INFO == "com.nvidia.spark.rapids.SparkRapidsBuildInfoEvent"


class TestOssRollingMarkers:
    """Test Apache Spark rolling event-log layout constants."""

    def test_eventlog_v2_prefix_exact(self):
        assert m.OSS_EVENT_LOG_DIR_PREFIX == "eventlog_v2_"

    def test_events_file_prefix_exact(self):
        assert m.OSS_EVENT_LOG_FILE_PREFIX == "events_"
