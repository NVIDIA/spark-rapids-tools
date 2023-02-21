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

"""Implementation of AWS storage related functionalities."""

import re
from dataclasses import dataclass

from spark_rapids_pytools.cloud_api.sp_types import CMDDriverBase
from spark_rapids_pytools.common.sys_storage import StorageDriver, FSUtil


@dataclass
class S3StorageDriver(StorageDriver):
    """
    Wrapper around aws-s3 commands such as copying/moving/listing files.
    """
    cli: CMDDriverBase

    @classmethod
    def get_cmd_prefix(cls):
        pref_arr = ['aws', 's3']
        return pref_arr[:]

    def resource_is_dir(self, src: str) -> bool:
        # if the resource is a directory, the S3 ls command would return PRE dir_name/
        if not src.startswith('s3://'):
            return super().resource_is_dir(src)
        # we do not want the resource name to be followed by a slash when we check whether
        # it is a directory.
        full_src = src if not src.endswith('/') else src[:-1]
        cmd_args = self.get_cmd_prefix()
        cmd_args.extend(['ls', full_src])
        # run command and make sure we return 0.
        res = False
        try:
            ls_out = self.cli.run_sys_cmd(cmd_args)
            folder_name = FSUtil.get_resource_name(src)
            matched_lines = re.findall(rf'(PRE)\s+({folder_name})/', ls_out)
            if len(matched_lines) > 0:
                res = True
        except RuntimeError:
            res = False
        return res

    def resource_exists(self, src) -> bool:
        if not src.startswith('s3://'):
            return super().resource_exists(src)
        # run s3 ls src if result is 0, then the resource exists
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
        if not src.startswith('s3://'):
            return super()._download_remote_resource(src, dest)
        # this is s3 storage
        cmd_args = self.get_cmd_prefix()
        cmd_args.extend(['cp', src, dest])
        if self.resource_is_dir(src):
            cmd_args.append('--recursive')
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_full_path(dest, FSUtil.get_resource_name(src))

    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None) -> str:
        if not dest.startswith('s3://'):
            return super()._upload_remote_dest(src, dest)
        # this is s3 storage
        cmd_args = self.get_cmd_prefix()
        cmd_args.extend(['cp', src, dest])
        if self.resource_is_dir(src):
            cmd_args.append('--recursive')
        if exclude_pattern:
            cmd_args.extend(['--exclude', exclude_pattern])
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_path(dest, FSUtil.get_resource_name(src))

    def is_file_path(self, value: str):
        if value.startswith('s3://'):
            return True
        return super().is_file_path(value)

    def _delete_path(self, src, fail_ok: bool = False):
        if not src.startswith('s3://'):
            super()._delete_path(src)
        else:
            cmd_args = self.get_cmd_prefix()
            cmd_args.extend(['rm', src])
            if self.resource_is_dir(src):
                cmd_args.append('--recursive')
            self.cli.run_sys_cmd(cmd_args)
