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
from spark_rapids_pytools.rapids.qualification import QualFilterApp, QualificationAsLocal, QualGpuClusterReshapeType


class CliOnpremLocalMode:  # pylint: disable=too-few-public-methods
    """
    A wrapper that runs the RAPIDS Accelerator tools locally on the dev machine for OnPrem
    platform. Apps are qualified based on speedup.
    """

    @staticmethod
    def qualification(cpu_cluster: str = None,
                      execution_cluster: str = None,
                      eventlogs: str = None,
                      local_folder: str = None,
                      remote_folder: str = None,
                      gpu_cluster: str = None,
                      tools_jar: str = None,
                      credentials_file: str = None,
                      filter_apps: str = QualFilterApp.tostring(QualFilterApp.SPEEDUPS),
                      gpu_cluster_recommendation: str = QualGpuClusterReshapeType.tostring(
                          QualGpuClusterReshapeType.get_default()),
                      jvm_heap_size: int = 24,
                      verbose: bool = False,
                      **rapids_options) -> None:
        if verbose:
            # when debug is set to true set it in the environment.
            ToolLogging.enable_debug_mode()
        wrapper_qual_options = {
            'platformOpts': {
                'credentialFile': credentials_file,
                'deployMode': DeployMode.LOCAL,
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
            'gpuClusterRecommendation': gpu_cluster_recommendation
        }
        tool_obj = QualificationAsLocal(platform_type=CloudPlatform.ONPREM,
                                        cluster=execution_cluster,
                                        output_folder=local_folder,
                                        wrapper_options=wrapper_qual_options,
                                        rapids_options=rapids_options)
        tool_obj.launch()


class OnPremWrapper:  # pylint: disable=too-few-public-methods
    """
    A wrapper script to run RAPIDS Accelerator tools (Qualification) on On-prem cluster.
    """
    def __init__(self):
        self.qualification = CliOnpremLocalMode.qualification
