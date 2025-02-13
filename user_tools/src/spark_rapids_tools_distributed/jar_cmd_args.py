# Copyright (c) 2025, NVIDIA CORPORATION.
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

""" Jar command arguments for running the Tools JAR on Spark """
from dataclasses import dataclass, field
from functools import cached_property
from typing import List


@dataclass
class JarCmdArgs:
    """
    Wrapper class to store the arguments required to run the Tools JAR on Spark.
    """
    jvm_args: List[str] = field(default=None, init=True)
    classpath_arr: List[str] = field(default=None, init=True)
    hadoop_classpath: str = field(default=None, init=True)
    jar_main_class: str = field(default=None, init=True)
    jar_output_dir_args: List[str] = field(default=None, init=True)
    extra_rapids_args: List[str] = field(default=None, init=True)

    @cached_property
    def jvm_log_file(self) -> str:
        """
        Return the log4j properties file from JVM arguments
        """
        for arg in self.jvm_args:
            if 'Dlog4j.configuration' in arg:
                return arg.split('=')[1]
        raise ValueError('log4j properties file not found in JVM arguments')

    @cached_property
    def tools_jar_path(self) -> str:
        """
        Tools JAR is the first element in the classpath array
        """
        return self.classpath_arr[1]

    @cached_property
    def rapids_args(self) -> List[str]:
        """
        Return the Rapids arguments
        """
        return self.extra_rapids_args[:-1]

    @cached_property
    def event_logs_path(self) -> str:
        """
        Return the path to the event logs directory
        """
        return self.extra_rapids_args[-1]
