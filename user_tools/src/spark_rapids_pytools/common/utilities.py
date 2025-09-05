# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
import threading
import time
import tempfile
from dataclasses import dataclass, field
from logging import Logger
from shutil import make_archive, which
from typing import Callable, Any, Optional

import chevron
from packaging.version import Version
from progress.spinner import PixelSpinner
from pygments import highlight
from pygments.formatters import get_formatter_by_name
from pygments.lexers import get_lexer_by_name


class Utils:
    """Utility class used to enclose common helpers and utilities."""
    warning_issued = False

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
        ts = datetime.datetime.now().astimezone().strftime('%Y%m%d%H%M%S')
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
    def get_sys_env_var(cls, k: str, def_val=None) -> Optional[str]:
        return os.environ.get(k, def_val)

    @classmethod
    def set_rapids_tools_env(cls, k: str, val):
        os.environ[cls.find_full_rapids_tools_env_key(k)] = str(val)

    @classmethod
    def get_or_set_rapids_tools_env(cls, k: str, default_val=None) -> Optional[str]:
        full_key = cls.find_full_rapids_tools_env_key(k)
        current_val = cls.get_sys_env_var(full_key, None)
        if current_val is None or (isinstance(current_val, str) and current_val == ''):
            if default_val is not None:
                cls.set_rapids_tools_env(k, default_val)
                return str(default_val)
        return current_val

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

    @classmethod
    def get_value_or_pop(cls, provided_value, options_dict, short_flag, default_value=None):
        """
        Gets a value or pops it from the provided options dictionary if the value is not explicitly provided.

        :param provided_value: The value to return if not None.
        :param options_dict: Dictionary containing options.
        :param short_flag: Flag to look for in options_dict.
        :param default_value: The default value to return if the target_key is not found. Defaults to None.
        :return: provided_value or the value from options_dict or the default_value.
        """
        if provided_value is not None:
            return provided_value
        if short_flag in options_dict:
            if not cls.warning_issued:
                cls.warning_issued = True
                print('Warning: Instead of using short flags for argument, consider providing the value directly.')
            return options_dict.pop(short_flag)
        return default_value


class ToolLogging:
    """Holds global utilities used for logging."""

    _logging_lock = threading.Lock()
    _current_debug_state = None

    @classmethod
    def _ensure_configured(cls, debug_enabled: bool = False):
        with cls._logging_lock:
            if cls._current_debug_state == debug_enabled:
                return
            logging.config.dictConfig(cls.get_log_dict({'debug': debug_enabled}))
            cls._current_debug_state = debug_enabled

    @classmethod
    def get_log_dict(cls, args):
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'simple': {
                    'format': '{asctime} {levelname} {run_id_tag} {name}: {message}',
                    'style': '{',
                    'datefmt': '%H:%M:%S',
                },
            },
            'filters': {
                'run_id': {
                    '()': 'spark_rapids_pytools.common.utilities.RunIdContextFilter'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                    'level': 'DEBUG' if args.get('debug') else 'ERROR',
                    'filters': ['run_id']
                },
            },
            'root': {
                'handlers': ['console'],
                'level': 'DEBUG',
            },
        }

    @classmethod
    def enable_debug_mode(cls):
        Utils.set_rapids_tools_env('LOG_DEBUG', 'True')

    @classmethod
    def is_debug_mode_enabled(cls):
        return Utils.get_or_set_rapids_tools_env('LOG_DEBUG')

    @classmethod
    def get_and_setup_logger(cls, type_label: str, debug_mode: bool = False):
        debug_enabled = bool(Utils.get_or_set_rapids_tools_env('LOG_DEBUG', debug_mode))

        cls._ensure_configured(debug_enabled)

        logger = logging.getLogger(type_label)

        # ToolLogging is a module level class
        # For multiple instances of another class(Profiling/Qualification),
        # the logger corresponding to that is registered only once.
        # So any new/updated FileHandler are not updated. Hence, we need to
        # rebind the FileHandler every time we get a logger instance for a type_label
        cls._rebind_file_handler(logger, Utils.get_or_set_rapids_tools_env('LOG_FILE'))
        return logger

    @classmethod
    def _rebind_file_handler(cls, logger: logging.Logger, log_file: str) -> None:
        # Remove existing FileHandlers to avoid stale paths and duplicates
        # Stale paths can occur if LOG_FILE env var changes between calls
        # or if multiple instances of Profiling/Qualification are created.
        for handler in list(logger.handlers):
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
                handler.close()
        # Attach a FileHandler if LOG_FILE is set
        if not log_file:
            return
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.addFilter(RunIdContextFilter())
        formatter = logging.Formatter(
            '{asctime} {levelname} {run_id_tag} {name}: {message}',
            style='{',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    @classmethod
    def modify_log4j_properties(cls, prop_file_path: str, new_log_file: str) -> str:
        """
        Modifies the log file path in a log4j properties file to redirect logging output to a new location.

        This method reads an existing log4j.properties file and alters the log file path specified
        for the FILE appender. The modified properties file is saved to a temporary file,
        which is returned to the caller to be used as the new log4j configuration. This temporary file
        is deleted after the java process is completed.

        :param prop_file_path: The file path to the original log4j.properties file. This file
                               should contain configurations for the log4j logging utility.
        :param new_log_file: The file path where the logging output is saved.

        :return str: The file path to the temporary modified log4j.properties file.
                     This temporary file retains the modifications and can be accessed until
                     explicitly deleted after the java process is completed.
        """
        with open(prop_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        with tempfile.NamedTemporaryFile(
                delete=False, mode='w+', suffix='.properties') as temp_file:
            for line in lines:
                if line.startswith('log4j.appender.FILE.File='):
                    temp_file.write(f'log4j.appender.FILE.File={new_log_file}\n')
                else:
                    temp_file.write(line)
        return temp_file.name


class RunIdContextFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """
    Pulls RUN_ID from environment; if absent, the tag is omitted entirely.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        run_id = Utils.get_or_set_rapids_tools_env('RUN_ID')
        tag = f' [{run_id}]' if run_id else ''
        setattr(record, 'run_id_tag', tag)
        return True


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
    logger: Logger = None
    res: int = field(default=0, init=False)
    out_std: str = field(default=None, init=False)
    err_std: str = field(default=None, init=False)
    timeout_secs: float = None

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
                               timeout=self.timeout_secs,
                               stdout=stdout,
                               stderr=stderr)
        else:
            # apply input to the command
            c = subprocess.run(actual_cmd,
                               executable='/bin/bash',
                               shell=True,
                               input=self.cmd_input,
                               text=True,
                               timeout=self.timeout_secs,
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
            processed_cmd_args = Utils.gen_joined_str(' ', process_credentials_option(cmd_args))
            cmd_err_msg = f'Error invoking CMD <{processed_cmd_args}>: {stderr_str}'
            raise RuntimeError(f'{cmd_err_msg}')

        self.out_std = c.stdout if isinstance(c.stdout, str) else c.stdout.decode('utf-8', errors='ignore')
        if self.process_streams_cb is not None:
            self.process_streams_cb(self.out_std, self.err_std)
        if self.out_std:
            return self.out_std.strip()
        return self.out_std

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.cmd')


@dataclass
class ToolsSpinner:
    """
    A class to manage the spinner animation.
    Reference: https://stackoverflow.com/a/66558182

    :param enabled: Flag indicating if the spinner is enabled. Defaults to True.
    """
    enabled: bool = field(default=True, init=True)
    pixel_spinner: PixelSpinner = field(default=PixelSpinner('Processing...', hide_cursor=False), init=False)
    end: str = field(default='Processing Completed!', init=False)
    timeout: float = field(default=0.1, init=False)
    completed: bool = field(default=False, init=False)
    spinner_thread: threading.Thread = field(default=None, init=False)
    pause_event: threading.Event = field(default=threading.Event(), init=False)

    def _spinner_animation(self):
        while not self.completed:
            self.pixel_spinner.next()
            time.sleep(self.timeout)
            while self.pause_event.is_set():
                self.pause_event.wait(self.timeout)

    def start(self):
        if self.enabled:
            self.spinner_thread = threading.Thread(target=self._spinner_animation, daemon=True)
            self.spinner_thread.start()
        return self

    def stop(self):
        self.completed = True
        print(f'\r\n{self.end}', flush=True)

    def pause(self, insert_newline=False):
        if self.enabled:
            if insert_newline:
                # Print a newline for visual separation
                print()
            self.pause_event.set()

    def resume(self):
        self.pause_event.clear()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, tb):
        self.stop()
