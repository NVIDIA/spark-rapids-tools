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

"""Definition of global utilities and helpers methods."""

import datetime
import logging.config
import os
import re
import secrets
import ssl
import string
import subprocess
import sys
import urllib
from dataclasses import dataclass, field
from logging import Logger
from shutil import which, make_archive
from typing import Callable, Any

import certifi
import chevron
from bs4 import BeautifulSoup
from packaging.version import Version
from pygments import highlight
from pygments.formatters import get_formatter_by_name
from pygments.lexers import get_lexer_by_name

from spark_rapids_pytools import get_version


class Utils:
    """Utility class used to enclose common helpers and utilities."""

    @classmethod
    def gen_random_string(cls, str_length: int) -> str:
        return ''.join(secrets.choice(string.hexdigits) for _ in range(str_length))

    @classmethod
    def gen_uuid_with_ts(cls, pref: str = None, suffix_len: int = 0) -> str:
        """
        Generate uuid in the form of YYYYmmddHHmmss
        :param pref:
        :param suffix_len:
        :return:
        """
        ts = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
        uuid_parts = [] if pref is None else [pref]
        uuid_parts.append(ts)
        if suffix_len > 0:
            uuid_parts.append(cls.gen_random_string(suffix_len))
        return Utils.gen_joined_str('_', uuid_parts)

    @classmethod
    def resource_path(cls, resource_name: str) -> str:
        # pylint: disable=import-outside-toplevel
        if sys.version_info < (3, 9):
            import importlib_resources
        else:
            import importlib.resources as importlib_resources

        pkg = importlib_resources.files('spark_rapids_pytools')
        return pkg / 'resources' / resource_name

    @classmethod
    def reformat_release_version(cls, defined_version: Version) -> str:
        # get the release from version
        version_tuple = defined_version.release
        version_comp = list(version_tuple)
        # release format is under url YY.MM.MICRO where MM is 02, 04, 06, 08, 10, and 12
        res = f'{version_comp[0]}.{version_comp[1]:02}.{version_comp[2]}'
        return res

    @classmethod
    def get_latest_available_jar_version(cls, url_base: str, loaded_version: str) -> str:
        """
        Given the defined version in the python tools build, we want to be able to get the highest
        version number of the jar available for download from the mvn repo.
        The returned version is guaranteed to be LEQ to the defined version. For example, it is not
        allowed to use jar version higher than the python tool itself.
        :param url_base: the base url from which the jar file is downloaded. It can be mvn repo.
        :param loaded_version: the version from the python tools in string format
        :return: the string value of the jar that should be downloaded.
        """
        context = ssl.create_default_context(cafile=certifi.where())
        defined_version = Version(loaded_version)
        jar_version = Version(loaded_version)
        version_regex = r'\d{2}\.\d{2}\.\d+'
        version_pattern = re.compile(version_regex)
        with urllib.request.urlopen(url_base, context=context) as resp:
            html_content = resp.read()
            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            # Find all the links with title in the format of "xx.xx.xx"
            links = soup.find_all('a', {'title': version_pattern})
            # Get the link with the highest value
            for link in links:
                curr_title = re.search(version_regex, link.get('title'))
                if curr_title:
                    curr_version = Version(curr_title.group())
                    if curr_version <= defined_version:
                        jar_version = curr_version
        # get formatted string
        return cls.reformat_release_version(jar_version)

    @classmethod
    def get_base_release(cls) -> str:
        """
        For now the tools_jar is always with major.minor.0.
        this method makes sure that even if the package version is incremented, we will still
        get the correct url.
        :return: a string containing the release number 22.12.0, 23.02.0, amd 23.04.0..etc
        """
        defined_version = Version(get_version(main=None))
        # get the release from version
        return cls.reformat_release_version(defined_version)

    @classmethod
    def is_system_tool(cls, tool_name: str) -> bool:
        """
        check whether a tool is installed on the system.
        :param tool_name: name of the tool to check
        :return: True or False
        """
        return which(tool_name) is not None

    @classmethod
    def make_archive(cls, base_name, fmt, root_dir) -> None:
        """
        check whether a tool is installed on the system.
        :param base_name: the name of the file to create
        :param format: the archive format: "zip", "tar", "gztar"
        :param root_dir: the root directory of the archive
        :return:
        """
        return make_archive(base_name=base_name, format=fmt, root_dir=root_dir)

    @classmethod
    def find_full_rapids_tools_env_key(cls, actual_key: str) -> str:
        return f'RAPIDS_USER_TOOLS_{actual_key}'

    @classmethod
    def get_sys_env_var(cls, k: str, def_val=None):
        return os.environ.get(k, def_val)

    @classmethod
    def get_rapids_tools_env(cls, k: str, def_val=None):
        val = cls.get_sys_env_var(cls.find_full_rapids_tools_env_key(k), def_val)
        return val

    @classmethod
    def set_rapids_tools_env(cls, k: str, val):
        os.environ[cls.find_full_rapids_tools_env_key(k)] = str(val)

    @classmethod
    def gen_str_header(cls, title: str, ruler='-', line_width: int = 40) -> str:
        dash = ruler * line_width
        return cls.gen_multiline_str('', dash, f'{title:^{line_width}}', dash)

    @classmethod
    def gen_report_sec_header(cls,
                              title: str,
                              ruler='-',
                              title_width: int = 20,
                              hrule: bool = True) -> str:
        line_width = max(title_width, len(title) + 1)
        if hrule:
            dash = ruler * line_width
            return cls.gen_multiline_str('', f'{title}:', dash)
        return cls.gen_multiline_str('', f'{title}:')

    @classmethod
    def gen_joined_str(cls, join_elem: str, items) -> str:
        """
        Given a variable length of String arguments (or list), returns a single string
        :param items: the items to be concatenated together. it could be a hybrid of str and lists
        :param join_elem: the character to use as separator of the join
        :return: a single string joining the items
        """
        res_arr = []
        for item in list(filter(lambda i: i is not None, items)):
            if isinstance(item, list):
                # that's an array
                res_arr.extend(list(filter(lambda i: i is not None, item)))
            else:
                res_arr.append(item)
        return join_elem.join(res_arr)

    @classmethod
    def gen_multiline_str(cls, *items) -> str:
        return cls.gen_joined_str(join_elem='\n', items=items)

    @classmethod
    def get_os_name(cls) -> str:
        return os.uname().sysname


class ToolLogging:
    """Holds global utilities used for logging."""

    @classmethod
    def get_log_dict(cls, args):
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'simple': {
                    'format': '{asctime} {levelname} {name}: {message}',
                    'style': '{',
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                },
            },
            'root': {
                'handlers': ['console'],
                'level': 'DEBUG' if args.get('debug') else 'INFO',
            },
        }

    @classmethod
    def enable_debug_mode(cls):
        Utils.set_rapids_tools_env('LOG_DEBUG', 'True')

    @classmethod
    def is_debug_mode_enabled(cls):
        return Utils.get_rapids_tools_env('LOG_DEBUG')

    @classmethod
    def get_and_setup_logger(cls, type_label: str, debug_mode: bool = False):
        debug_enabled = bool(Utils.get_rapids_tools_env('LOG_DEBUG', debug_mode))
        logging.config.dictConfig(cls.get_log_dict({'debug': debug_enabled}))
        logger = logging.getLogger(type_label)
        log_file = Utils.get_rapids_tools_env('LOG_FILE')
        if log_file:
            # create file handler which logs even debug messages
            fh = logging.FileHandler(log_file)
            # TODO: set the formatter and handler for file logging
            # fh.setLevel(log_level)
            # fh.setFormatter(ExtraLogFormatter())
            logger.addHandler(fh)
        return logger


class TemplateGenerator:
    """A class to manage templates and content generation"""
    @classmethod
    def render_template_file(cls, fpath: string, template_args: dict) -> str:
        with open(fpath, 'r', encoding='UTF-8') as f:
            return chevron.render(f, data=template_args)

    @classmethod
    def highlight_bash_code(cls, bash_script: str) -> str:
        return highlight(bash_script, get_lexer_by_name('Bash'), get_formatter_by_name('terminal'))


@dataclass
class SysCmd:
    """
    Run command and check return code, capture output etc.
    """
    cmd: Any = None
    cmd_input: str = None
    env_vars: dict = None
    expected: int = 0
    fail_ok: bool = False
    process_streams_cb: Callable = None
    logger: Logger = ToolLogging.get_and_setup_logger('rapids.tools.cmd')
    res: int = field(default=0, init=False)
    out_std: str = field(default=None, init=False)
    err_std: str = field(default=None, init=False)

    def has_failed(self):
        return self.expected != self.res and not self.fail_ok

    def build(self, field_values: dict = None):
        if field_values is not None:
            for field_name in field_values:
                setattr(self, field_name, field_values.get(field_name))
        return self

    def _process_env_vars(self):
        sys_env_vars = []
        if self.env_vars is not None:
            for env_k, env_arg in self.env_vars.items():
                val = f'{env_k}={env_arg}'
                sys_env_vars.append(val)
        return sys_env_vars

    def exec(self) -> str:
        def process_credentials_option(cmd: list):
            res = []
            for i, arg in enumerate(cmd):
                if 'account-key' in cmd[i - 1]:
                    arg = 'MY_ACCESS_KEY'
                elif 'fs.azure.account.key' in arg:
                    arg = arg.split('=')[0] + '=MY_ACCESS_KEY'
                res.append(arg)
            return res

        # pylint: disable=subprocess-run-check
        if isinstance(self.cmd, str):
            cmd_args = [self.cmd]
        else:
            cmd_args = self.cmd[:]
        if ToolLogging.is_debug_mode_enabled():
            # do not dump the entire command to debugging to avoid exposing the env-variables
            self.logger.debug('submitting system command: <%s>',
                              Utils.gen_joined_str(' ', process_credentials_option(cmd_args)))
        full_cmd = self._process_env_vars()
        full_cmd.extend(cmd_args)
        actual_cmd = Utils.gen_joined_str(' ', full_cmd)
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE
        # pylint: disable=subprocess-run-check
        if self.cmd_input is None:
            c = subprocess.run(actual_cmd,
                               executable='/bin/bash',
                               shell=True,
                               stdout=stdout,
                               stderr=stderr)
        else:
            # apply input to the command
            c = subprocess.run(actual_cmd,
                               executable='/bin/bash',
                               shell=True,
                               input=self.cmd_input,
                               text=True,
                               stdout=stdout,
                               stderr=stderr)
        self.res = c.returncode
        # pylint: enable=subprocess-run-check
        self.err_std = c.stderr if isinstance(c.stderr, str) else c.stderr.decode('utf-8', errors='ignore')
        if self.has_failed():
            std_error_lines = [f'\t| {line}' for line in self.err_std.splitlines()]
            stderr_str = ''
            if len(std_error_lines) > 0:
                error_lines = Utils.gen_multiline_str(std_error_lines)
                stderr_str = f'\n{error_lines}'
            processed_cmd_args = process_credentials_option(cmd_args)
            cmd_err_msg = f'Error invoking CMD <{Utils.gen_joined_str(" ", processed_cmd_args)}>: {stderr_str}'
            raise RuntimeError(f'{cmd_err_msg}')

        self.out_std = c.stdout if isinstance(c.stdout, str) else c.stdout.decode('utf-8', errors='ignore')
        if self.process_streams_cb is not None:
            self.process_streams_cb(self.out_std, self.err_std)
        if self.out_std:
            return self.out_std.strip()
        return self.out_std
