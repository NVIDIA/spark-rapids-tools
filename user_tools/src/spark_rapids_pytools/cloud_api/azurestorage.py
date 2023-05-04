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
from spark_rapids_pytools.common.sys_storage import StorageDriver, FSUtil


@dataclass
class AzureStorageDriver(StorageDriver):
    """
    Wrapper around azure commands such as copying/moving/listing files.
    """
    cli: CMDDriverBase

    @classmethod
    def get_cmd_prefix(cls):
        pref_arr = ['azcopy']
        return pref_arr[:]
    
    def resource_is_dir(self, src: str) -> bool:
        if not src.startswith('https://'):
            return super().resource_is_dir(src)
        cmd_args = self.get_cmd_prefix()
        # TODO: append SAS token
        cmd_args.extend(['list', src])
        try:
            std_out = self.cli.run_sys_cmd(cmd_args)
            stdout_lines = std_out.splitlines()
            if stdout_lines:
                for out_line in stdout_lines:
                    if src in out_line:
                        # if any path contains the directory path return True
                        return True
        except RuntimeError:
            self.cli.logger.debug('Error in checking resource [%s] is directory', src)
        return False
    
    def resource_exists(self, src) -> bool:
        if not src.startswith('https://'):
            return super().resource_exists(src)
        # run 'azcopy list src' if result is 0, then the resource exists.
        cmd_args = self.get_cmd_prefix()
        # TODO: append SAS token
        cmd_args.extend(['list', src])
        # run command and make sure we return 0.
        try:
            self.cli.run_sys_cmd(cmd_args)
            res = True
        except RuntimeError:
            res = False
        return res
    
    def _download_remote_resource(self, src: str, dest: str) -> str:
        if not src.startswith('https://'):
            return super()._download_remote_resource(src, dest)
        # this is azure data lake storage
        cmd_args = self.get_cmd_prefix()
        # TODO: append SAS token
        cmd_args.extend(['cp', src, dest])
        if self.resource_is_dir(src):
            cmd_args.append('--recursive=true')
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_full_path(dest, FSUtil.get_resource_name(src))
    
    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None) -> str:
        if not dest.startswith('https://'):
            return super()._upload_remote_dest(src, dest)
        # this is azure data lake storage
        cmd_args = self.get_cmd_prefix()
        cmd_args.extend(['cp', src, dest])
        if self.resource_is_dir(src):
            cmd_args.append('--recursive=true')
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_path(dest, FSUtil.get_resource_name(src))
    
    def is_file_path(self, value: str):
        if value.startswith('https://'):
            return True
        return super().is_file_path(value)
    
    def _delete_path(self, src, fail_ok: bool = False):
        if not src.startswith('https://'):
            super()._delete_path(src)
        else:
            cmd_args = self.get_cmd_prefix()
            cmd_args.extend(['rm', src])
            if self.resource_is_dir(src):
                cmd_args.append('--recursive')
            self.cli.run_sys_cmd(cmd_args)
