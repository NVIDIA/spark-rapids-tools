# Copyright (c) 2024, NVIDIA CORPORATION.
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

"""Utility functions for network functions"""

import concurrent
import dataclasses
import datetime
import json
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor
from functools import cached_property
from typing import Optional, List

from fastcore.all import urlsave
from fastprogress.fastprogress import progress_bar
from pydantic import Field, AnyUrl
from pydantic.dataclasses import dataclass
from typing_extensions import Annotated

from spark_rapids_tools import CspPath
from spark_rapids_tools.storagelib import LocalPath, CspFs
from spark_rapids_tools.storagelib.tools.fs_utils import FileVerificationResult


def fast_download_url(url: str, fpath: str, timeout=None, pbar_enabled=False) -> str:
    """
    Download the given url and display a progress bar
    """
    pbar = progress_bar([])

    def progress_bar_cb(count=1, bsize=1, total_size=None):
        pbar.total = total_size
        pbar.update(count * bsize)

    return urlsave(url, fpath, reporthook=progress_bar_cb if pbar_enabled else None, timeout=timeout)


def default_download_options(custom_opts: dict = None) -> dict:
    """
    Utility function to create the default options for the download.
    :param custom_opts: allows user to override or extend the download options.
    :return: A dictionary with the default options + custom options.
    """
    custom_opts = custom_opts if custom_opts else {}
    default_opts = {
        # force the download even teh file exists
        'forceDownload': False,
        # 3600 is 1 hour
        'timeOut': 3600,
        # number of retries
        'retryCount': 3
    }
    default_opts.update(custom_opts)
    return default_opts


def default_verification_options(custom_opts: dict = None) -> dict:
    """
    Utility function to create the default options for the verification.
    :param custom_opts: Allows user to override and to extend the default verification options.
    :return: A dictionary with the default options + custom options.
    """
    custom_opts = custom_opts if custom_opts else {}
    default_opts = {
        # the file must exist
        'must_exist': True,
        # it has to be a file
        'is_file': True,
        # size of the file. 0 to ignore
        'size': 0,
        # list of extensions to check
        'extensions': []
    }
    default_opts.update(custom_opts)
    return default_opts


def download_exception_handler(future: Future) -> None:
    # Handle any exceptions raised by the task
    exception = future.exception()
    if exception:
        print('Error while downloading dependency: %s', exception)


@dataclasses.dataclass
class DownloadResult:
    """
    A class that represents the result of a download task. It contains the following information:
    :param resource: The path where the downloaded resource is located. Notice that we use a CspPath
          instead of a string or FilePath to represent different schemes and CspStorages.
    :param origin_url: The original URL of the resource.
    :param success: Whether the download is successful.
    :param downloaded: Whether the resource is downloaded or loaded from an existing folder.
          i.e., if the file already exists, and the download is not enforced, then the downloaded
          value should be false.
    :param download_time: The elapsed download time in seconds.
    :param verified: Whether the resource has been verified.
    :param download_error: The error that occurred during the download if any.
    """
    resource: CspPath
    origin_url: str
    success: bool
    downloaded: bool
    download_time: float = 0
    verified: bool = True
    download_error: Optional[Exception] = None

    def pretty_print(self) -> str:
        json_str = {
            'resource': str(self.resource),
            'origin_url': self.origin_url,
            'success': self.success,
            'downloaded': self.downloaded,
            'download_time(seconds)':  f'{self.download_time:,.3f}',
        }
        if self.download_error:
            json_str['download_error'] = str(self.download_error)

        return json.dumps(json_str, indent=4)


@dataclass
class DownloadTask:
    """
    A class that represents a download task. It contains the following information:
    :param src_url: The URL of the resource to download.
    :param dest_folder: The destination folder where the resource is downloaded.
    :param configs: A dictionary of download options. See default_download_options() for more
        information about acceptable options.
    :param verification: A dictionary of verification options. See default_verification_options()
        for more information about acceptable options.
    """
    src_url: AnyUrl
    dest_folder: str
    configs: Annotated[Optional[dict], Field(default_factory=lambda: default_download_options())]  # pylint: disable=unnecessary-lambda
    verification: Annotated[Optional[dict], Field(default_factory=lambda: default_verification_options())]  # pylint: disable=unnecessary-lambda

    def __post_init__(self):
        # Add the defaults when the caller does not set default values.
        self.configs = default_download_options(self.configs)
        self.verification = default_verification_options(self.verification)

    @cached_property
    def resource_base_name(self) -> str:
        return CspPath(self.src_url.path).base_name()

    @cached_property
    def dest_dir(self) -> LocalPath:
        dest_root = CspPath(self.dest_folder)
        dest_root.create_dirs(exist_ok=True)
        return dest_root

    @cached_property
    def dest_res(self) -> CspPath:
        return self.dest_dir.create_sub_path(self.resource_base_name)

    @cached_property
    def force_download(self) -> bool:
        return self.configs.get('forceDownload', False)

    def _download_resource(self, opts: dict) -> DownloadResult:
        """
        Downloads a single Url path to a local file system.
        :param opts: Options passed to the internal download call.
        :return: A DownloadResult object.
        """
        def download_from_weburl() -> None:
            fast_download_url(opts['srcUrl'], opts['destPath'], timeout=opts['timeOut'])

        def download_from_csfs() -> None:
            csp_src = CspPath(opts['srcUrl'])
            CspFs.copy_file(csp_src, self.dest_res)

        start_time = time.monotonic()
        curr_time_stamp = datetime.datetime.now().timestamp()
        download_exception = None
        downloaded = False
        success = False
        try:
            if self.src_url.scheme == 'https':
                download_from_weburl()
            else:
                download_from_csfs()
            FileVerificationResult(res_path=self.dest_res, opts=self.verification, raise_on_error=True)
            # update modified time and access time
            os.utime(opts['destPath'], times=(curr_time_stamp, curr_time_stamp))
            success = True
            downloaded = True
        except Exception as e:    # pylint: disable=broad-except
            download_exception = e
        return DownloadResult(resource=self.dest_res,
                              origin_url=opts['srcUrl'],
                              success=success,
                              downloaded=downloaded,
                              download_time=time.monotonic() - start_time,
                              download_error=download_exception)

    def run_task(self) -> DownloadResult:
        local_res = self.dest_res
        if local_res.exists() and not self.force_download:
            # verify that the file is correct using the verification options
            if FileVerificationResult(res_path=self.dest_res,
                                      opts=self.verification, raise_on_error=False).successful:
                # the file already exists. Skip downloading it.
                return DownloadResult(resource=local_res, origin_url=str(self.src_url),
                                      success=True, downloaded=False)
        # the file needs to be redownloaded. For example, it might be a corrupted attempt.
        download_opts = {
            'srcUrl': str(self.src_url),
            'destPath': local_res.no_scheme,
            # set default timeout oif a single task to 1 hour
            'timeOut': self.configs.get('timeOut', 3600),
        }
        download_res = self._download_resource(download_opts)
        return download_res

    def async_submit(self, thread_executor: ThreadPoolExecutor, task_list: List[Future]) -> None:
        futures = thread_executor.submit(self.run_task)
        futures.add_done_callback(download_exception_handler)
        task_list.append(futures)


@dataclass
class DownloadManager:
    """
    A class that downloads a list of resources in parallel. It creates a threadPool and run a
    DownloadTask for each.
    https://stackoverflow.com/questions/6509261/how-to-use-concurrent-futures-with-timeouts
    :param download_tasks: A list of DownloadTask objects.
    :param max_workers: The maximum workers threads to run in parallel.
        To disable parallelism, set the value to 1.
    :param time_out: The maximum time to wait for the download to complete. Default is 30 minutes.
       Note that there is a bug in the python module that ignores the timeout. The timeout exception
       is not triggered for some reason. Nevertheless, we set the timeout hopefully this bug gets fixed.
       The current behavior is to throws an exception after all the future tasks get completed.

    example usage:
    ```py
        DownloadManager(
        [DownloadTask(src_url='https://urlpath/rapids-4-spark-tools_2.12-24.08.2.jar',
                      dest_folder='file:///var/tmp/spark_cache_folder_test/async',
                      configs={'forceDownload': True},
                      verification={'size': ....}),
         DownloadTask(src_url='https://urlpath/rapids-4-spark-tools_2.12-24.08.1.jar',
                      dest_folder='file:///var/tmp/spark_cache_folder_test/async',
                      configs={'forceDownload': True},
                      verification={'file_hash': FileHashAlgorithm(HashAlgorithm('md5'), '.....')}),
         # the following is file-to-file copy.
         DownloadTask(src_url='file:///path/to/file.ext',
                      dest_folder='file:///var/tmp/spark_cache_folder_test/async'),
         DownloadTask(src_url='https://urlpath/spark-3.5.0-bin-hadoop3.tgz',
                      dest_folder='file:///var/tmp/spark_cache_folder_test/async',
                      configs={'forceDownload': True},
                      verification={'file_hash': FileHashAlgorithm(HashAlgorithm('sha512'), '....')})
         ]).submit()
    ```
    """
    download_tasks: List[DownloadTask]
    # set it to 1 to avoid parallelism
    max_workers: Optional[int] = 4
    # set the timeout to 60 minutes.
    time_out: Optional[int] = 3600

    def submit(self) -> List[DownloadResult]:
        futures_list = []
        results = []
        final_results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                for task in self.download_tasks:
                    task.async_submit(executor, futures_list)
                # set the timeout to 30 minutes.
                for future in concurrent.futures.as_completed(futures_list, timeout=self.time_out):
                    results.append(future.result())
                final_results = [res for res in results if res is not None]
            except concurrent.futures.TimeoutError as e:
                print('Time out waiting for as_completed()', e)
        return final_results
