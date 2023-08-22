import os
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Set

from spark_rapids_pytools.common.sys_storage import FSUtil


class OfflineDownloader:
    def __init__(self, resource_dir: str):
        self.SUPPORTED_PLATFORMS = ["emr", "databricks_aws", "databricks_azure", "dataproc", "onprem"]
        self.CONFIGS_SUFFIX = "-configs.json"
        self.RESOURCE_DIR = resource_dir
        self.MVN_BASE_URL = 'https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12'
        self.RAPIDS_VERSION = '23.06.4'
        self.RAPIDS_URL = '{}/{}/rapids-4-spark-tools_2.12-{}.jar'.format(self.MVN_BASE_URL,
                                                                          self.RAPIDS_VERSION,
                                                                          self.RAPIDS_VERSION)

    def fetch_dependencies(self) -> Set[str]:
        """
        Fetches the dependency information from configuration files for each supported platform.
        Returns a set of dependency URIs.
        """
        dependency_uris = set()
        dependency_uris.add(self.RAPIDS_URL) # Add RAPIDS JAR as dependency
        for platform in self.SUPPORTED_PLATFORMS:
            config_file = os.path.join(self.RESOURCE_DIR, f"{platform}{self.CONFIGS_SUFFIX}")
            with open(config_file, 'r') as file:
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
        offline_dir = os.path.join(self.RESOURCE_DIR, 'offline')

        def download_dependency(dependency_uri):
            resource_file_name = FSUtil.get_resource_name(dependency_uri)
            resource_file_path = os.path.join(offline_dir, resource_file_name)

            print(f"Downloading dependency: {resource_file_name}")
            FSUtil.fast_download_url(dependency_uri, resource_file_path)
            FSUtil.fast_download_url(dependency_uri + '.asc', resource_file_path, pbar_enabled=False)

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
        print(f"Usage: python {sys.argv[0]} <resource_dir>")
        sys.exit(1)

    input_resource_dir = sys.argv[1]
    downloader = OfflineDownloader(input_resource_dir)
    downloader.run()
