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

    def resource_is_dir(self, src: str) -> bool:
        if not src.startswith('gs://'):
            return super().resource_is_dir(src)
        full_src = src if src.endswith('/') else f'{src}/'
        cmd_args = ['gsutil',
                    '-q',
                    'stat',
                    full_src]
        # run command and make sure we return 0.
        try:
            self.cli.run_sys_cmd(cmd_args)
            res = True
        except RuntimeError:
            res = False
        return res

    def resource_exists(self, src) -> bool:
        if not src.startswith('gs://'):
            return super().resource_exists(src)
        # run gsutil -q stat src if result is 0, then the resource exists
        cmd_args = ['gsutil',
                    '-q',
                    'stat',
                    src]
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
        res_is_dir = self.resource_is_dir(src)
        recurse_arg = '-r' if res_is_dir else ''
        cmd_args = ['gsutil',
                    'cp',
                    recurse_arg,
                    src,
                    dest]
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_full_path(dest, FSUtil.get_resource_name(src))

    def _upload_remote_dest(self, src: str, dest: str, exclude_pattern: str = None) -> str:
        if not dest.startswith('gs://'):
            return super()._upload_remote_dest(src, dest)
        # this is gstorage
        res_is_dir = self.resource_is_dir(src)
        recurse_arg = '-r' if res_is_dir else ''
        cmd_args = ['gsutil',
                    'cp',
                    recurse_arg,
                    src,
                    dest]
        self.cli.run_sys_cmd(cmd_args)
        return FSUtil.build_path(dest, FSUtil.get_resource_name(src))

    def is_file_path(self, value: str):
        if value.startswith('gs://'):
            return True
        return super().is_file_path(value)
