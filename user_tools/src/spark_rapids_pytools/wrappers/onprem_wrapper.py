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


"""Wrapper class to run tools associated with RAPIDS Accelerator for Apache Spark plugin on On-Prem cluster."""

from spark_rapids_pytools.cloud_api.sp_types import DeployMode, CloudPlatform
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.profiling import ProfilingAsLocal
from spark_rapids_pytools.rapids.qualification import QualFilterApp, QualificationAsLocal, QualGpuClusterReshapeType


class CliOnpremLocalMode:  # pylint: disable=too-few-public-methods
    """
    A wrapper that runs the RAPIDS Accelerator tools locally on the dev machine for OnPrem
    platform. Apps are qualified based on speedup.
    """

    @staticmethod
    def qualification(cpu_cluster: str = None,
                      eventlogs: str = None,
                      local_folder: str = None,
                      remote_folder: str = None,
                      gpu_cluster: str = None,
                      tools_jar: str = None,
                      credentials_file: str = None,
                      filter_apps: str = QualFilterApp.tostring(QualFilterApp.SPEEDUPS),
                      target_platform: str = None,
                      gpu_cluster_recommendation: str = QualGpuClusterReshapeType.tostring(
                          QualGpuClusterReshapeType.get_default()),
                      jvm_heap_size: int = 24,
                      verbose: bool = False,
                      **rapids_options) -> None:
        if verbose:
            # when debug is set to true set it in the environment.
            ToolLogging.enable_debug_mode()
        # if target_platform is specified, check if it's valid supported platform and filter the
        # apps based on savings
        if target_platform is not None:
            if CliOnpremLocalMode.is_target_platform_supported(target_platform):
                if cpu_cluster is None:
                    raise RuntimeError('OnPrem\'s cluster property file required to calculate'
                                       'savings for ' + target_platform + ' platform')
                filter_apps: str = QualFilterApp.tostring(QualFilterApp.SAVINGS)
            else:
                raise RuntimeError(target_platform + ' platform is currently not supported to calculate savings'
                                   ' from OnPrem cluster')

        wrapper_qual_options = {
            'platformOpts': {
                'credentialFile': credentials_file,
                'deployMode': DeployMode.LOCAL,
                'targetPlatform': target_platform
            },
            'migrationClustersProps': {
                'cpuCluster': cpu_cluster,
                'gpuCluster': gpu_cluster
            },
            'jobSubmissionProps': {
                'remoteFolder': remote_folder,
                'platformArgs': {
                    'jvmMaxHeapSize': jvm_heap_size
                }
            },
            'eventlogs': eventlogs,
            'filterApps': filter_apps,
            'toolsJar': tools_jar,
            'gpuClusterRecommendation': gpu_cluster_recommendation,
            'target_platform': target_platform
        }
        tool_obj = QualificationAsLocal(platform_type=CloudPlatform.ONPREM,
                                        output_folder=local_folder,
                                        wrapper_options=wrapper_qual_options,
                                        rapids_options=rapids_options)
        tool_obj.launch()

    @staticmethod
    def is_target_platform_supported(target_platform: str):
        return target_platform == 'dataproc'

    @staticmethod
    def profiling(worker_info: str = None,
                  eventlogs: str = None,
                  local_folder: str = None,
                  tools_jar: str = None,
                  jvm_heap_size: int = 24,
                  verbose: bool = False,
                  **rapids_options) -> None:
        """
        The Profiling tool analyzes both CPU or GPU generated event logs and generates information
        which can be used for debugging and profiling Apache Spark applications.

        :param  worker_info: A path pointing to a yaml file containing the system information of a
        worker node. It is assumed that all workers are homogenous.
        If missing, it throws an error.
        :param  eventlogs: Event log filenames or directories containing event logs (comma separated).
        :param local_folder: Local work-directory path to store the output and to be used as root
        directory for temporary folders/files. The final output will go into a subdirectory called
        ${local_folder}/prof-${EXEC_ID} where exec_id is an auto-generated unique identifier of the
        execution. If the argument is NONE, the default value is the env variable
        RAPIDS_USER_TOOLS_OUTPUT_DIRECTORY if any; or the current working directory.
        :param tools_jar: Path to a bundled jar including Rapids tool. The path is a local filesystem,
        or remote gs url. If missing, the wrapper downloads the latest rapids-4-spark-tools_*.jar
        from maven repo
        :param verbose: True or False to enable verbosity to the wrapper script
        :param jvm_heap_size: The maximum heap size of the JVM in gigabytes
        :param rapids_options: A list of valid Profiling tool options.
        Note that the wrapper ignores ["output-directory", "worker-info"] flags, and it does not support
        multiple "spark-property" arguments.
        For more details on Profiling tool options, please visit
        https://nvidia.github.io/spark-rapids/docs/spark-profiling-tool.html#profiling-tool-options
        """

        if worker_info is None:
            raise RuntimeError('worker_info.yaml file containing the system information of '
                               'a worker node is required to run profiling tool on OnPrem '
                               'cluster')
        if verbose:
            # when debug is set to true set it in the environment.
            ToolLogging.enable_debug_mode()
        wrapper_prof_options = {
            'platformOpts': {
                'deployMode': DeployMode.LOCAL,
                'targetPlatform': CloudPlatform.ONPREM
            },
            'jobSubmissionProps': {
                'platformArgs': {
                     'jvmMaxHeapSize': jvm_heap_size
                }
            },
            'eventlogs': eventlogs,
            'toolsJar': tools_jar,
            'autoTunerFileInput': worker_info
        }
        ProfilingAsLocal(platform_type=CloudPlatform.ONPREM,
                         output_folder=local_folder,
                         wrapper_options=wrapper_prof_options,
                         rapids_options=rapids_options).launch()


class OnPremWrapper:  # pylint: disable=too-few-public-methods
    """
    A wrapper script to run RAPIDS Accelerator tools (Qualification, Profiling) on On-prem cluster.
    """
    def __init__(self):
        self.qualification = CliOnpremLocalMode.qualification
        self.profiling = CliOnpremLocalMode.profiling
