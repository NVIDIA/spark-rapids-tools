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
import secrets
import string
import subprocess
import sys
from logging import Logger
from typing import Callable, Any
from dataclasses import dataclass, field


def gen_random_string(str_length: int) -> str:
    return ''.join(secrets.choice(string.hexdigits) for _ in range(str_length))


def gen_uuid_with_ts(pref: str = None, suffix_len:  int = 0) -> str:
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
        uuid_parts.append(gen_random_string(suffix_len))
    return '_'.join(uuid_parts)


def resource_path(resource_name: str) -> str:
    # pylint: disable=import-outside-toplevel
    if sys.version_info < (3, 9):
        import importlib_resources
    else:
        import importlib.resources as importlib_resources

    pkg = importlib_resources.files('pyrapids')
    return pkg / 'resources' / resource_name


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
        set_rapids_tools_env('LOG_DEBUG', 'True')

    @classmethod
    def is_debug_mode_enabled(cls):
        return get_rapids_tools_env('LOG_DEBUG')

    @classmethod
    def get_and_setup_logger(cls, type_label: str, debug_mode: bool = False):
        debug_enabled = bool(get_rapids_tools_env('LOG_DEBUG', debug_mode))
        logging.config.dictConfig(cls.get_log_dict({'debug': debug_enabled}))
        logger = logging.getLogger(type_label)
        log_file = get_rapids_tools_env('LOG_FILE')
        if log_file:
            # create file handler which logs even debug messages
            fh = logging.FileHandler(log_file)
            # TODO: set the formatter and handler for file logging
            # fh.setLevel(log_level)
            # fh.setFormatter(ExtraLogFormatter())
            logger.addHandler(fh)
        return logger


def find_full_rapids_tools_env_key(actual_key: str) -> str:
    return f'RAPIDS_USER_TOOLS_{actual_key}'


def get_sys_env_var(k: str, def_val=None):
    return os.environ.get(k, def_val)


def get_rapids_tools_env(k: str, def_val=None):
    val = get_sys_env_var(find_full_rapids_tools_env_key(k), def_val)
    return val


def set_rapids_tools_env(k: str, val):
    os.environ[find_full_rapids_tools_env_key(k)] = str(val)


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
        # pylint: disable=subprocess-run-check
        if ToolLogging.is_debug_mode_enabled():
            self.logger.debug('submitting system command: <%s>', self.cmd)
        if isinstance(self.cmd, str):
            cmd_args = [self.cmd]
        else:
            cmd_args = self.cmd[:]
        full_cmd = self._process_env_vars()
        full_cmd.extend(cmd_args)
        actual_cmd = ' '.join(full_cmd)
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
        if self.has_failed():
            stderror_content = c.stderr if isinstance(c.stderr, str) else c.stderr.decode('utf-8')
            std_error_lines = [f'\t| {line}' for line in stderror_content.splitlines()]
            stderr_str = ''
            if len(std_error_lines) > 0:
                error_lines = '\n'.join(std_error_lines)
                stderr_str = f'\n{error_lines}'
            cmd_err_msg = f'Error invoking CMD <{cmd_args}>: {stderr_str}'
            raise RuntimeError(f'{cmd_err_msg}')

        self.out_std = c.stdout if isinstance(c.stdout, str) else c.stdout.decode('utf-8')
        self.err_std = c.stderr if isinstance(c.stderr, str) else c.stderr.decode('utf-8')
        if self.process_streams_cb is not None:
            self.process_streams_cb(self.out_std, self.err_std)
        return self.out_std
