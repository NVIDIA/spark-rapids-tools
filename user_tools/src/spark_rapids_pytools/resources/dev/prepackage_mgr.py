# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
This module defines the logic to fetch all dependencies and resources required to run the user-tools
without need to access the web during runtime.
"""

import os
import shutil
import tarfile
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import fire

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.rapids.rapids_tool import RapidsTool
from spark_rapids_tools import CspEnv
from spark_rapids_tools.utils import Utilities

# Defines the constants and static configurations
prepackage_conf = {
    '_supported_platforms': [csp.value for csp in CspEnv if csp != CspEnv.NONE],
    '_configs_suffix': '-configs.json',
    '_mvn_base_url': 'https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark-tools_2.12',
    '_folder_name': 'csp-resources'
}


class PrepackageMgr:   # pylint: disable=too-few-public-methods
    """ Class that handles downloading dependencies to be pre-packaged ahead of runtime.

    The script can be triggered by passing the arguments and the properties of the class
    For example:

    $> python prepackage_mgr.py --resource_dir=RESOURCE_DIR --tools_jar=TOOLS_JAR run
    $> python prepackage_mgr.py run --resource_dir=RESOURCE_DIR --tools_jar=TOOLS_JAR

    For more information:

    $> python prepackage_mgr.py --help

    :param resource_dir: Root folder where the configuration files are located.
    :param dest_dir: Directory in which the resources are downloaded and stored.
           If missing, the tool creates a subfolder '$resource_dir/csp-resources'.
           Warning: the 'dest_dir' may be deleted when 'archive_enabled' is set to True.
    :param tools_jar: Path of the spark-rapids-user-tools jar file. Typically, this is the snapshot
           jar file of the current build.
    :param archive_enabled: A flag to enable/disable compressing the resources.
           Note that a wheel-package with compressed prepackaged resources is 30% less size
           compared to the non-compressed one (~ 600 Vs. 900 MB).
           When enabled, the prepackaged-resources are stored in '$resource_dir/csp-resources.tgz'.
           If the 'dest_dir' is provided, then the output is stored as '$dest_dir/../csp-resources.tgz'
    """

    def __init__(self,
                 resource_dir: str,
                 dest_dir: str = None,
                 tools_jar: str = None,
                 archive_enabled: bool = True):
        for field_name in prepackage_conf:
            setattr(self, field_name, prepackage_conf.get(field_name))
        self.resource_dir = resource_dir
        self.dest_dir = dest_dir
        self.tools_jar = tools_jar
        self.archive_enabled = archive_enabled
        # process the arguments for default values
        print(f'Resource directory is: {self.resource_dir}')
        print(f'tools_jar = {tools_jar}')
        self.resource_dir = FSUtil.get_abs_path(self.resource_dir)
        if self.dest_dir is None:
            self.dest_dir = FSUtil.build_full_path(self.resource_dir, self._folder_name)  # pylint: disable=no-member
        else:
            self.dest_dir = FSUtil.get_abs_path(self.dest_dir)

    def _get_spark_rapids_jar_url(self) -> str:
        jar_version = Utilities.get_latest_mvn_jar_from_metadata(self._mvn_base_url)  # pylint: disable=no-member
        return (f'{self._mvn_base_url}/'  # pylint: disable=no-member
                f'{jar_version}/rapids-4-spark-tools_2.12-{jar_version}.jar')

    def _fetch_resources(self) -> dict:
        """
        Fetches the resource information from configuration files for each supported platform.
        Returns a dictionary of resource details.
        """
        resource_uris = {}

        # Add RAPIDS JAR as dependency
        if self.tools_jar:
            # copy from existing file. replace snapshot
            jar_file_name = FSUtil.get_resource_name(self.tools_jar)
            FSUtil.make_dirs(self.dest_dir)
            dest_file = FSUtil.build_path(self.dest_dir, jar_file_name)
            shutil.copy2(self.tools_jar, dest_file)
        else:
            # get the latest tools_jar from mvn
            rapids_url = self._get_spark_rapids_jar_url()
            rapids_name = FSUtil.get_resource_name(rapids_url)
            resource_uris[rapids_url] = {'name': rapids_name, 'pbar_enabled': False}

        for platform in self._supported_platforms:  # pylint: disable=no-member
            config_file = FSUtil.build_full_path(self.resource_dir,
                                                 f'{platform}{self._configs_suffix}')  # pylint: disable=no-member
            platform_conf = JSONPropertiesContainer(config_file)
            dependency_list = RapidsTool.get_rapids_tools_dependencies('LOCAL', platform_conf)
            for dependency in dependency_list:
                uri = dependency.get('uri')
                name = FSUtil.get_resource_name(uri)
                if uri:
                    resource_uris[uri] = {'name': name, 'pbar_enabled': False}
                    resource_uris[uri + '.asc'] = {'name': name + '.asc', 'pbar_enabled': False}

            # Add pricing files as resources
            if platform_conf.get_value_silent('pricing'):
                for pricing_entry in platform_conf.get_value('pricing', 'catalog', 'onlineResources'):
                    uri = pricing_entry.get('onlineURL')
                    name = pricing_entry.get('localFile')
                    if uri and name:
                        resource_uris[uri] = {'name': name, 'pbar_enabled': False}

        return resource_uris

    def _download_resources(self, resource_uris: dict):
        resource_uris_list = list(resource_uris.items())

        def download_task(resource_uri, resource_info):
            resource_name = resource_info['name']
            pbar_enabled = resource_info['pbar_enabled']
            resource_file_path = FSUtil.build_full_path(self.dest_dir, resource_name)

            print(f'Downloading {resource_name}')
            FSUtil.fast_download_url(resource_uri, resource_file_path, pbar_enabled=pbar_enabled)

        with ThreadPoolExecutor() as executor:
            executor.map(lambda x: download_task(x[0], x[1]), resource_uris_list)

    def _compress_resources(self) -> Optional[str]:
        if not self.archive_enabled:
            return self.dest_dir
        root_dir = os.path.dirname(self.dest_dir)
        tar_file = FSUtil.build_full_path(root_dir, f'{self._folder_name}.tgz')  # pylint: disable=no-member
        print('Creating archive.....')
        with tarfile.open(tar_file, 'w:gz') as tarhandle:
            tarhandle.add(self.dest_dir, arcname='.')
            tarhandle.close()
        print('Created archived resources successfully')
        # delete the csp-resources folder
        FSUtil.remove_path(self.dest_dir)
        return tar_file

    def run(self):
        """
        Main method to fetch and download dependencies.
        """
        resources_to_download = self._fetch_resources()
        self._download_resources(resources_to_download)
        output_res = self._compress_resources()
        print(f'CSP-prepackaged resources stored as {output_res}')


if __name__ == '__main__':
    fire.Fire(PrepackageMgr)
