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

"""Implementation of Azure Data Lake Storage with ABFS (Azure Blob File System) related functionalities."""

from dataclasses import dataclass, field
from logging import Logger

from spark_rapids_pytools.cloud_api.sp_types import CMDDriverBase
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import StorageDriver, FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.storagelib.adls.adlspath import AdlsPath


@dataclass
class AzureStorageDriver(StorageDriver):
    """
    Wrapper around azure commands such as copying/moving/listing files.
    """
    cli: CMDDriverBase
    account_keys: dict = field(default_factory=dict, init=False)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.azurestoragedriver'), init=False)

    @classmethod
    def get_cmd_prefix(cls):
        pref_arr = ['az storage fs']
        return pref_arr[:]

    @classmethod
    def get_file_system(cls, url: str):
        return url.split('@')[0].split('://')[1]

    @classmethod
    def get_path(cls, url: str):
        return url.split('dfs.core.windows.net')[1]

    def get_account_key(self, account_name: str):
        if account_name in self.account_keys:
            return self.account_keys[account_name]

        try:
            cmd_args = ['az storage account show-connection-string', '--name', account_name]
            std_out = self.cli.run_sys_cmd(cmd_args)
            conn_str = JSONPropertiesContainer(prop_arg=std_out, file_load=False).get_value('connectionString')
            key = conn_str.split('AccountKey=')[1].split(';')[0]
            self.account_keys[account_name] = key
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.info('Error retrieving access key for storage account %s: %s', account_name, ex)
            key = ''

        return key

    def resource_is_dir(self, src: str) -> bool:
        if not src.startswith('abfss://'):
            return super().resource_is_dir(src)

        try:
            file_system = self.get_file_system(src)
            account_name = AdlsPath.get_abfs_account_name(src)
            path = self.get_path(src)

            cmd_args = self.get_cmd_prefix()
            cmd_args.extend(['file list', '-f', file_system, '--account-name', account_name])
            if path:
                cmd_args.extend(['--path', path])

            account_key = self.get_account_key(account_name)
            if account_key:
                cmd_args.extend(['--account-key', account_key])

            std_out = self.cli.run_sys_cmd(cmd_args)
            stdout_info = JSONPropertiesContainer(prop_arg=std_out, file_load=False)
            path = path.lstrip('/')

            if not (len(stdout_info.props) == 1 and stdout_info.props[0]['name'] == path):  # not a file
                return True
        except RuntimeError:
            self.cli.logger.debug('Error in checking resource [%s] is directory', src)
        return False

    def resource_exists(self, src) -> bool:
        if not src.startswith('abfss://'):
            return super().resource_exists(src)

        # run 'az storage fs file list' if result is 0, then the resource exists.
        try:
            file_system = self.get_file_system(src)
            account_name = AdlsPath.get_abfs_account_name(src)
            path = self.get_path(src)

            cmd_args = self.get_cmd_prefix()
            cmd_args.extend(['file list', '-f', file_system, '--account-name', account_name])
            if path:
                cmd_args.extend(['--path', path])

            account_key = self.get_account_key(account_name)
            if account_key:
                cmd_args.extend(['--account-key', account_key])

            self.cli.run_sys_cmd(cmd_args)
            res = True
        except RuntimeError:
            res = False
        return res

    def _download_remote_resource(self, src: str, dest: str) -> str:
        if not src.startswith('abfss://'):
            return super()._download_remote_resource(src, dest)
        # this is azure data lake storage
        file_system = self.get_file_system(src)
        account_name = AdlsPath.get_abfs_account_name(src)
        path = self.get_path(src)

        cmd_args = self.get_cmd_prefix()
        if self.resource_is_dir(src):
            cmd_args.extend(['directory download', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-s', path, '-d', dest, '--recursive'])
        else:
            cmd_args.extend(['file download', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-p', path, '-d', dest])

        account_key = self.get_account_key(account_name)
        if account_key:
            cmd_args.extend(['--account-key', account_key])

        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_full_path(dest, FSUtil.get_resource_name(src))

    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None) -> str:
        if not dest.startswith('abfss://'):
            return super()._upload_remote_dest(src, dest)
        # this is azure data lake storage
        file_system = self.get_file_system(dest)
        account_name = AdlsPath.get_abfs_account_name(dest)
        dest_path = self.get_path(dest)
        src_resource_name = FSUtil.get_resource_name(src)
        dest_resource_name = FSUtil.get_resource_name(dest)

        cmd_args = self.get_cmd_prefix()
        #  source is a directory
        if self.resource_is_dir(src):
            # for azure cli, specifying a directory to copy will result in a duplicate; so we will double-check
            # that if the dest already has the name of the src, then we move level up.
            if src_resource_name == dest_resource_name:
                # go to the parent level for destination
                dest_path = dest_path.split(src_resource_name)[0].rstrip('/')
            cmd_args.extend(['directory upload', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-s', src, '-d', dest_path, '--recursive'])
        else:  # source is a file
            cmd_args.extend(['file upload', '-f', file_system, '--account-name', account_name])
            # dest is a directory, we will append the source resource name to it
            if self.resource_is_dir(dest):
                dest_path = dest_path if dest_path[-1] == '/' else dest_path + '/'
                dest_path = dest_path + src_resource_name
            cmd_args.extend(['-s', src, '-p', dest_path])

        account_key = self.get_account_key(account_name)
        if account_key:
            cmd_args.extend(['--account-key', account_key])

        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_path(dest, FSUtil.get_resource_name(src))

    def is_file_path(self, value: str):
        if value.startswith('https://'):
            return True
        return super().is_file_path(value)

    def _delete_path(self, src, fail_ok: bool = False):
        if not src.startswith('abfss://'):
            super()._delete_path(src)
            return

        # this is azure data lake storage
        file_system = self.get_file_system(src)
        account_name = AdlsPath.get_abfs_account_name(src)
        path = self.get_path(src)

        cmd_args = self.get_cmd_prefix()
        if self.resource_is_dir(src):
            cmd_args.extend(['directory delete', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-n', path, '-y'])
        else:
            cmd_args.extend(['file delete', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-p', path, '-y'])

        account_key = self.get_account_key(account_name)
        if account_key:
            cmd_args.extend(['--account-key', account_key])

        self.cli.run_sys_cmd(cmd_args)
