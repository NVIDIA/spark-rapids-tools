# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

"""Wrapper for the Hadoop File system"""

from typing import Any

from pyarrow.fs import HadoopFileSystem

from ..cspfs import CspFs, register_fs_class, BoundedArrowFsT


@register_fs_class("hdfs", "HadoopFileSystem")
class HdfsFs(CspFs):
    """Implementation of FileSystem for HAdoopFileSystem on top of pyArrow
    (Docstring copied from pyArrow.HadoopFileSystem)

    The HadoopFileSystem is initialized with the following list of arguments:

    >>> HadoopFileSystem(unicode host, int port=8020, unicode user=None, *, int replication=3,
    ...     int buffer_size=0, default_block_size=None, kerb_ticket=None, extra_conf=None)

    The libhdfs library is loaded at runtime (rather than at link / library load time, since the
    library may not be in your LD_LIBRARY_PATH), and relies on some environment variables.
    HADOOP_HOME: the root of your installed Hadoop distribution. Often has lib/native/libhdfs.so.
    JAVA_HOME: the location of your Java SDK installation.
    ARROW_LIBHDFS_DIR (optional): explicit location of libhdfs.so if it is installed somewhere
        other than $HADOOP_HOME/lib/native.
    CLASSPATH: must contain the Hadoop jars.
    example to set the export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
    """

    @classmethod
    def create_fs_handler(cls, *args: Any, **kwargs: Any) -> BoundedArrowFsT:
        try:
            return HadoopFileSystem(*(args or ("default",)), **kwargs)
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError(f"Failed to create HadoopFileSystem handler: {e}") from e
