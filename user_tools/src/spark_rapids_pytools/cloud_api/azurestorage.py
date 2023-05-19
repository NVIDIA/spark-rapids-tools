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

from dataclasses import dataclass

from spark_rapids_pytools.cloud_api.sp_types import CMDDriverBase
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import StorageDriver, FSUtil


@dataclass
class AzureStorageDriver(StorageDriver):
    """
    Wrapper around azure commands such as copying/moving/listing files.
    """
    cli: CMDDriverBase

    @classmethod
    def get_cmd_prefix(cls):
        pref_arr = ['az storage fs']
        return pref_arr[:]

    @classmethod
    def get_file_system(cls, url: str):
        return url.split('@')[0].split('://')[1]

    @classmethod
    def get_account_name(cls, url: str):
        return url.split('@')[1].split('.')[0]

    @classmethod
    def get_path(cls, url: str):
        return url.split('dfs.core.windows.net')[1]

    def resource_is_dir(self, src: str) -> bool:
        if not src.startswith('abfss://'):
            return super().resource_is_dir(src)
        cmd_args = self.get_cmd_prefix()

        file_system = self.get_file_system(src)
        account_name = self.get_account_name(src)
        path = self.get_path(src)

        cmd_args.extend(['file list', '-f', file_system, '--account-name', account_name])
        if len(path) > 0:
            cmd_args.extend(['--path', path])

        try:
            std_out = self.cli.run_sys_cmd(cmd_args)
            stdout_info = JSONPropertiesContainer(prop_arg=std_out, file_load=False)
            if len(stdout_info.props) != 1:
                # not a file
                return True
        except RuntimeError:
            self.cli.logger.debug('Error in checking resource [%s] is directory', src)
        return False

    def resource_exists(self, src) -> bool:
        if not src.startswith('abfss://'):
            return super().resource_exists(src)
        # run 'az storage fs file list' if result is 0, then the resource exists.
        cmd_args = self.get_cmd_prefix()

        file_system = self.get_file_system(src)
        account_name = self.get_account_name(src)
        path = self.get_path(src)

        cmd_args.extend(['file list', '-f', file_system, '--account-name', account_name])
        if len(path) > 0:
            cmd_args.extend(['--path', path])

        # run command and make sure we return 0.
        try:
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
        account_name = self.get_account_name(src)
        path = self.get_path(src)

        cmd_args = self.get_cmd_prefix()

        if self.resource_is_dir(src):
            cmd_args.extend(['directory download', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-s', path, '-d', dest, '--recursive'])
        else:
            cmd_args.extend(['file download', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-p', path, '-d', dest])
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_full_path(dest, FSUtil.get_resource_name(src))

    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None) -> str:
        if not dest.startswith('abfss://'):
            return super()._upload_remote_dest(src, dest)
        # this is azure data lake storage
        file_system = self.get_file_system(dest)
        account_name = self.get_account_name(dest)
        path = self.get_path(dest)

        cmd_args = self.get_cmd_prefix()

        if self.resource_is_dir(src):
            cmd_args.extend(['directory upload', '-f', file_system, '--account-name', account_name])
            cmd_args.extend(['-s', src, '-d', path, '--recursive'])
        else:
            if self.resource_is_dir(dest):
                if not path[-1] == '/':
                    path = path + '/'
                path = path + FSUtil.get_resource_name(src)
                cmd_args.extend(['directory upload', '-f', file_system, '--account-name', account_name])
                cmd_args.extend(['-s', src, '-d', path])
            else:
                cmd_args.extend(['file upload', '-f', file_system, '--account-name', account_name])
                cmd_args.extend(['-s', src, '-p', path])
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_path(dest, FSUtil.get_resource_name(src))

    def is_file_path(self, value: str):
        if value.startswith('https://'):
            return True
        return super().is_file_path(value)

    def _delete_path(self, src, fail_ok: bool = False):
        if not src.startswith('abfss://'):
            super()._delete_path(src)
        else:
            cmd_args = self.get_cmd_prefix()
            # this is azure data lake storage
            file_system = self.get_file_system(src)
            account_name = self.get_account_name(src)
            path = self.get_path(src)

            if self.resource_is_dir(src):
                cmd_args.extend(['directory delete', '-f', file_system, '--account-name', account_name])
                cmd_args.extend(['-n', path, '-y'])
            else:
                cmd_args.extend(['file delete', '-f', file_system, '--account-name', account_name])
                cmd_args.extend(['-p', path, '-y'])

            self.cli.run_sys_cmd(cmd_args)
