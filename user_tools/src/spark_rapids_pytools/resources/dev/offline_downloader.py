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

"""
This module defines the OfflineDownloader class for downloading dependencies.
"""
import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Set

from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils


class OfflineDownloader:
    """
    Class for downloading dependencies for offline usage.
    """
    supported_platforms: list = ['emr', 'databricks_aws', 'databricks_azure', 'dataproc', 'onprem']
    configs_suffix: str = '-configs.json'
    mvn_base_url: str = 'https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12'

    def __init__(self, resource_dir: str):
        self.resource_dir = resource_dir

    def get_spark_rapids_jar_url(self) -> str:
        jar_version = Utils.get_latest_available_jar_version(self.mvn_base_url,
                                                             Utils.get_base_release())
        return f'{self.mvn_base_url}/{jar_version}/rapids-4-spark-tools_2.12-{jar_version}.jar'

    def fetch_dependencies(self) -> Set[str]:
        """
        Fetches the dependency information from configuration files for each supported platform.
        Returns a set of dependency URIs.
        """
        dependency_uris = set()
        dependency_uris.add(self.get_spark_rapids_jar_url())  # Add RAPIDS JAR as dependency
        for platform in self.supported_platforms:
            config_file = os.path.join(self.resource_dir, f'{platform}{self.configs_suffix}')
            with open(config_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
                dependencies = data['dependencies']
                dependency_list = dependencies['deployMode']['LOCAL']
                for dependency in dependency_list:
                    uri = dependency['uri']
                    dependency_uris.add(uri)
        return dependency_uris

    def download_dependencies(self, dependency_uris: Set[str]):
        """
        Downloads the given dependencies and their signature files to the offline directory.
        :param dependency_uris: Set of dependency URIs.
        """
        offline_dir = os.path.join(self.resource_dir, 'offline')

        def download_dependency(dependency_uri):
            resource_file_name = FSUtil.get_resource_name(dependency_uri)
            resource_file_path = os.path.join(offline_dir, resource_file_name)

            print(f'Downloading dependency: {resource_file_name}')
            FSUtil.fast_download_url(dependency_uri, resource_file_path)
            FSUtil.fast_download_url(dependency_uri + '.asc', resource_file_path + '.asc', pbar_enabled=False)

        with ThreadPoolExecutor() as executor:
            executor.map(download_dependency, dependency_uris)

    def run(self):
        """
        Main method to fetch and download dependencies.
        """
        dependencies_to_download = self.fetch_dependencies()
        self.download_dependencies(dependencies_to_download)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Usage: python {sys.argv[0]} <resource_dir>')
        sys.exit(1)

    input_resource_dir = sys.argv[1]
    downloader = OfflineDownloader(input_resource_dir)
    downloader.run()
