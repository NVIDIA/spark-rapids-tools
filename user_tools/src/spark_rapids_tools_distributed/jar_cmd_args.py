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

""" Jar command arguments for running the Tools JAR on Spark """
from dataclasses import dataclass, field
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
