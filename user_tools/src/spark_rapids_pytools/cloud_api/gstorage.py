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

"""Implementation of google storage related functionalities."""

from dataclasses import dataclass

from spark_rapids_pytools.cloud_api.sp_types import CMDDriverBase
from spark_rapids_pytools.common.sys_storage import StorageDriver, FSUtil


@dataclass
class GStorageDriver(StorageDriver):
    """
    Wrapper around gsutil commands such as copying/moving/listing files.
    """
    cli: CMDDriverBase

    @classmethod
    def get_cmd_prefix(cls):
        pref_arr = ['gsutil']
        return pref_arr[:]

    @classmethod
    def get_cmd_cp_prefix(cls, is_dir: bool):
        """
        Note that using -m flag for multithreaded processing of copying cause the process to hang
        forever. So, we overcome this by limiting the parallel process property in each command
        """
        if not is_dir:
            return ['gsutil', 'cp']
        # the bug is more emphasized on mac OS (Utils.get_os_name() == 'Darwin')
        return ['gsutil', '-o', '\"GSUtil:parallel_process_count=3\"', '-m', 'cp', '-r']

    def resource_is_dir(self, src: str) -> bool:
        if not src.startswith('gs://'):
            return super().resource_is_dir(src)
        # for gsutil, running ls command on a file, will return an output string that has
        # the same resource.
        # if the resource is a directory, the output will contain an extra slash at the end.
        cmd_args = self.get_cmd_prefix()
        cmd_args.extend(['ls', src])
        try:
            std_out = self.cli.run_sys_cmd(cmd_args)
            stdout_lines = std_out.splitlines()
            for out_line in stdout_lines:
                if out_line.endswith('/') and out_line.startswith(src):
                    return True
        except RuntimeError:
            self.cli.logger.debug('Error in checking resource [%s] is directory', src)
        return False

    def resource_exists(self, src) -> bool:
        if not src.startswith('gs://'):
            return super().resource_exists(src)
        # run gsutil ls src if result is 0, then the resource exists.
        cmd_args = self.get_cmd_prefix()
        cmd_args.extend(['ls', src])
        # run command and make sure we return 0.
        try:
            self.cli.run_sys_cmd(cmd_args)
            res = True
        except RuntimeError:
            res = False
        return res

    def _download_remote_resource(self, src: str, dest: str) -> str:
        if not src.startswith('gs://'):
            return super()._download_remote_resource(src, dest)
        # this is gstorage
        cmd_args = self.get_cmd_cp_prefix(self.resource_is_dir(src))
        cmd_args.extend([src, dest])
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_full_path(dest, FSUtil.get_resource_name(src))

    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None) -> str:
        if not dest.startswith('gs://'):
            return super()._upload_remote_dest(src, dest)
        # this is gstorage
        cmd_args = self.get_cmd_cp_prefix(self.resource_is_dir(src))
        cmd_args.extend([src, dest])
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_path(dest, FSUtil.get_resource_name(src))

    def is_file_path(self, value: str):
        if value.startswith('gs://'):
            return True
        return super().is_file_path(value)

    def _delete_path(self, src, fail_ok: bool = False):
        if not src.startswith('gs://'):
            super()._delete_path(src)
        else:
            res_is_dir = self.resource_is_dir(src)
            recurse_arg = '-r' if res_is_dir else ''
            cmd_args = self.get_cmd_prefix()
            cmd_args.extend(['rm', recurse_arg, src])
            self.cli.run_sys_cmd(cmd_args)
