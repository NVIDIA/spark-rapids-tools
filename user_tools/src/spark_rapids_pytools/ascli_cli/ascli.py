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

"""CLI to run tools associated with RAPIDS Accelerator for Apache Spark plugin."""

import fire

from spark_rapids_pytools.cloud_api.sp_types import CloudPlatform, DeployMode
from spark_rapids_pytools.rapids.profiling import ProfilingAsLocal
from spark_rapids_pytools.rapids.qualification import QualGpuClusterReshapeType, QualFilterApp, QualificationAsLocal
from spark_rapids_pytools.wrappers.onprem_wrapper import CliOnpremLocalMode


class ASCLIWrapper(object):  # pylint: disable=too-few-public-methods
    """CLI to run tools associated with RAPIDS Accelerator for Apache Spark plugin.

    A wrapper script to run RAPIDS Accelerator tools (Qualification, Profiling, and Bootstrap)
    locally on the dev machine.
    """

    def qualification(self,
                      eventlogs: str = None,
                      cluster: str = None,
                      platform: str = CloudPlatform.tostring(CloudPlatform.get_default()),
                      output_folder: str = None,
                      target_platform: str = None,
                      filter_apps: str = QualFilterApp.tostring(QualFilterApp.SAVINGS),
                      gpu_cluster_recommendation: str = QualGpuClusterReshapeType.tostring(
                          QualGpuClusterReshapeType.get_default())):
        """Provides a wrapper to simplify the execution of RAPIDS Qualification tool.

        The Qualification tool analyzes Spark events generated from CPU based Spark applications to
        help quantify the expected acceleration and costs savings of migrating a Spark application
        or query to GPU. The wrapper downloads dependencies and executes the analysis on the local
        dev machine.

        :param eventlogs: Event log filenames or CSP storage directories containing event logs
                (comma separated).

                Skipping this argument requires that the cluster argument points to a valid
                cluster name on the CSP.
        :param cluster: Name of cluster or path to cluster-properties. Note that using a "file path"
                requires the `platform` argument.
        :param platform: defines one of the following "onprem", "emr", "dataproc", "databricks-aws",
                and "databricks-azure".
        :param output_folder: path to store the output
        :param target_platform: Cost savings and speedup recommendation for comparable cluster in
                target_platform based on on-premises cluster configuration.
                Requires cluster.
        :param gpu_cluster_recommendation: The type of GPU cluster recommendation to generate.
                Requires "Cluster".
                It accepts one of the following:

                "MATCH": keep GPU cluster same number of nodes as CPU cluster;
                "CLUSTER": recommend optimal GPU cluster by cost for entire cluster;
                "JOB": recommend optimal GPU cluster by cost per job
        :param filter_apps:  filtering criteria of the applications listed in the final STDOUT table
                is one of the following (NONE, SPEEDUPS, savings).
                Requires "Cluster".

                Note that this filter does not affect the CSV report.
                "NONE" means no filter applied. "SPEEDUPS" lists all the apps that are either
                'Recommended', or 'Strongly Recommended' based on speedups. "SAVINGS"
                lists all the apps that have positive estimated GPU savings except for the apps that
                are "Not Applicable"
        """
        runtime_platform = CloudPlatform.fromstring(platform)
        if runtime_platform == CloudPlatform.ONPREM:
            # if target_platform is specified, check if it's valid supported platform and filter the
            # apps based on savings
            if target_platform is not None:
                if CliOnpremLocalMode.is_target_platform_supported(target_platform):
                    if cluster is None:
                        raise RuntimeError('OnPrem\'s cluster property file required to calculate'
                                           'savings for ' + target_platform + ' platform')
                else:
                    raise RuntimeError(f'The platform [{target_platform}] is currently not supported'
                                       'to calculate savings from OnPrem cluster')
            else:
                # For onPRem runtime, the filter should be reset to speedups when no target_platform
                # is defined
                filter_apps = QualFilterApp.tostring(QualFilterApp.SPEEDUPS)

        wrapper_qual_options = {
            'platformOpts': {
                'credentialFile': None,
                'deployMode': DeployMode.LOCAL,
                'targetPlatform': target_platform
            },
            'migrationClustersProps': {
                'cpuCluster': cluster,
                'gpuCluster': None
            },
            'jobSubmissionProps': {
                'remoteFolder': None,
                'platformArgs': {
                    'jvmMaxHeapSize': 24
                }
            },
            'eventlogs': eventlogs,
            'filterApps': filter_apps,
            'toolsJar': None,
            'gpuClusterRecommendation': gpu_cluster_recommendation,
            'target_platform': target_platform
        }
        tool_obj = QualificationAsLocal(platform_type=runtime_platform,
                                        output_folder=output_folder,
                                        wrapper_options=wrapper_qual_options)
        return tool_obj.launch()

    def profiling(self,
                  eventlogs: str = None,
                  cluster: str = None,
                  platform: str = CloudPlatform.tostring(CloudPlatform.get_default()),
                  output_folder: str = None):
        """Provides a wrapper to simplify the execution of RAPIDS Profiling tool.

        The Profiling tool analyzes GPU event logs and generates information
        which can be used for debugging and profiling Apache Spark applications.

        :param eventlogs: Event log filenames or CSP storage directories containing event logs
                (comma separated).

                Skipping this argument requires that the cluster argument points to a valid
                cluster name on the CSP.
        :param cluster: The cluster on which the Apache Spark applications were executed.
                It can either be a CSP-cluster name or a path to the cluster/worker's info properties
                file (json format).
        :param platform: defines one of the following "onprem", "emr", "dataproc", "databricks-aws",
                and "databricks-azure".
        :param output_folder: path to store the output
        """

        wrapper_prof_options = {
            'platformOpts': {
                'credentialFile': None,
                'deployMode': DeployMode.LOCAL,
            },
            'migrationClustersProps': {
                'gpuCluster': cluster
            },
            'jobSubmissionProps': {
                'remoteFolder': None,
                'platformArgs': {
                    'jvmMaxHeapSize': 24
                }
            },
            'eventlogs': eventlogs,
            'toolsJar': None,
            'autoTunerFileInput': None
        }
        prof_tool_obj = ProfilingAsLocal(platform_type=CloudPlatform.fromstring(platform),
                                         output_folder=output_folder,
                                         wrapper_options=wrapper_prof_options)
        return prof_tool_obj.launch()


def main():
    fire.Fire(ASCLIWrapper())


if __name__ == '__main__':
    main()
