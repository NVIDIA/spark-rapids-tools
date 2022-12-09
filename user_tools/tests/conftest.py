# Copyright (c) 2022, NVIDIA CORPORATION.
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
"""Add common helpers and utilities"""

import yaml


def mock_cluster_props(cluster: str, **unused_kwargs):
    with open(f'tests/resources/{cluster}.yaml', 'r', encoding='utf-8') as yaml_file:
        static_properties = yaml.safe_load(yaml_file)
        return yaml.dump(static_properties)


def get_wrapper_work_dir(tool_name, root_dir):
    """
    Given the tmp path of the unit tests, get the working directory of the wrapper.
    :param tool_name: the name of tool (qualification/profiling/bootstrap)
    :param root_dir: the tmp path of the unit test
    :return: the working directory of the wrapper
    """
    # TODO: load constants from the configuration files src/resources/*.yaml.
    return f'{root_dir}/wrapper-output/rapids_user_tools_{tool_name}'
