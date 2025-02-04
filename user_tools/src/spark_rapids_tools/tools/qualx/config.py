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
Config module for Qualx, controlled by environment variables.

Environment variables:
- QUALX_CACHE_DIR: cache directory for saving Profiler output.
- QUALX_DATA_DIR: data directory containing eventlogs, primarily used in dataset JSON files.
- QUALX_DIR: root directory for Qualx execution, primarily used in dataset JSON files to locate
    dataset-specific plugins.
- QUALX_LABEL: targeted label column for XGBoost model.
- SPARK_RAPIDS_TOOLS_JAR: path to Spark RAPIDS Tools JAR file.
"""
import os


def get_cache_dir() -> str:
    """Get cache directory to save Profiler output."""
    return os.environ.get('QUALX_CACHE_DIR', 'qualx_cache')


def get_label() -> str:
    """Get targeted label column for XGBoost model."""
    label = os.environ.get('QUALX_LABEL', 'Duration')
    assert label in ['Duration', 'duration_sum']
    return label
