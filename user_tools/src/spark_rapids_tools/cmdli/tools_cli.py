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

from spark_rapids_tools.enums import QualGpuClusterReshapeType
from spark_rapids_tools.utils.util import gen_app_banner, init_environment
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.bootstrap import Bootstrap
from spark_rapids_pytools.rapids.profiling import ProfilingAsLocal
from spark_rapids_pytools.rapids.qualification import QualificationAsLocal
from .argprocessor import AbsToolUserArgModel


class ToolsCLI(object):  # pylint: disable=too-few-public-methods
    """CLI that provides a runtime environment that simplifies running cost and performance analysis
    using the RAPIDS Accelerator for Apache Spark.

    A wrapper script to run RAPIDS Accelerator tools (Qualification, Profiling, and Bootstrap)
    locally on the dev machine.
    """

    def qualification(self,
                      eventlogs: str = None,
                      cluster: str = None,
                      platform: str = None,
                      target_platform: str = None,
                      output_folder: str = None,
                      filter_apps: str = None,
                      cpu_cluster_price: float = None,
                      estimated_gpu_cluster_price: float = None,
                      cpu_discount: int = None,
                      gpu_discount: int = None,
                      global_discount: int = None,
                      gpu_cluster_recommendation: str = QualGpuClusterReshapeType.tostring(
                          QualGpuClusterReshapeType.get_default()),
                      verbose: bool = False,
                      **rapids_options):
        """The Qualification cmd provides estimated running costs and speedups by migrating Apache
        Spark applications to GPU accelerated clusters.

        The Qualification cmd analyzes Spark eventlogs generated from  CPU based Spark applications to
        help quantify the expected acceleration and costs savings of migrating a Spark application or
        query to GPU.
        The cmd will process each app individually, but will group apps with the same name into the
        same output row after averaging duration metrics accordingly.

        :param eventlogs: Event log filenames or CSP storage directories containing event logs
                (comma separated).

                Skipping this argument requires that the cluster argument points to a valid
                cluster name on the CSP.
        :param cluster: Name or ID (for databricks platforms) of cluster or path to cluster-properties.
        :param platform: defines one of the following "onprem", "emr", "dataproc", "dataproc-gke",
               "databricks-aws", and "databricks-azure".
        :param target_platform: Cost savings and speedup recommendation for comparable cluster in
                target_platform based on on-premises cluster configuration.

                Currently only `dataproc` is supported for target_platform.
                If not provided, the final report will be limited to GPU speedups only without
                cost-savings.
        :param output_folder: path to store the output
        :param filter_apps: filtering criteria of the applications listed in the final STDOUT table
                is one of the following (ALL, SPEEDUPS, SAVINGS).
                Requires "Cluster".

                Note that this filter does not affect the CSV report.
                "ALL" means no filter applied. "SPEEDUPS" lists all the apps that are either
                'Recommended', or 'Strongly Recommended' based on speedups. "SAVINGS"
                lists all the apps that have positive estimated GPU savings except for the apps that
                are "Not Applicable"
        :param cpu_cluster_price: the CPU cluster hourly price provided by the user.
        :param estimated_gpu_cluster_price: the GPU cluster hourly price provided by the user.
        :param cpu_discount: A percent discount for the cpu cluster cost in the form of an integer value
                (e.g. 30 for 30% discount).
        :param gpu_discount: A percent discount for the gpu cluster cost in the form of an integer value
                (e.g. 30 for 30% discount).
        :param global_discount: A percent discount for both the cpu and gpu cluster costs in the form of an
                integer value (e.g. 30 for 30% discount).
        :param gpu_cluster_recommendation: The type of GPU cluster recommendation to generate.
                Requires "Cluster".

                It accepts one of the following:
                "MATCH": keep GPU cluster same number of nodes as CPU cluster;
                "CLUSTER": recommend optimal GPU cluster by cost for entire cluster;
                "JOB": recommend optimal GPU cluster by cost per job
        :param verbose: True or False to enable verbosity of the script.
        :param rapids_options: A list of valid Qualification tool options.
                Note that the wrapper ignores ["output-directory", "platform"] flags, and it does not support
                multiple "spark-property" arguments.
                For more details on Qualification tool options, please visit
                https://docs.nvidia.com/spark-rapids/user-guide/latest/spark-qualification-tool.html#qualification-tool-options
        """
        if verbose:
            ToolLogging.enable_debug_mode()
        init_environment('qual')
        qual_args = AbsToolUserArgModel.create_tool_args('qualification',
                                                         eventlogs=eventlogs,
                                                         cluster=cluster,
                                                         platform=platform,
                                                         target_platform=target_platform,
                                                         output_folder=output_folder,
                                                         filter_apps=filter_apps,
                                                         cpu_cluster_price=cpu_cluster_price,
                                                         estimated_gpu_cluster_price=estimated_gpu_cluster_price,
                                                         cpu_discount=cpu_discount,
                                                         gpu_discount=gpu_discount,
                                                         global_discount=global_discount,
                                                         gpu_cluster_recommendation=gpu_cluster_recommendation)
        if qual_args:
            tool_obj = QualificationAsLocal(platform_type=qual_args['runtimePlatform'],
                                            output_folder=qual_args['outputFolder'],
                                            wrapper_options=qual_args,
                                            rapids_options=rapids_options)
            tool_obj.launch()

    def profiling(self,
                  eventlogs: str = None,
                  cluster: str = None,
                  platform: str = None,
                  output_folder: str = None,
                  verbose: bool = False,
                  **rapids_options):
        """The Profiling cmd provides information which can be used for debugging and profiling
        Apache Spark applications running on accelerated GPU cluster.

        The Profiling tool analyzes both CPU or GPU generated eventlogs and generates information
        including the Spark version, executor details, properties, etc.
        The tool also will recommend setting for the application assuming that the job will be able
        to use all the cluster resources (CPU and GPU) when it is running.

        :param eventlogs: Event log filenames or cloud storage directories
                containing event logs (comma separated). If missing, the wrapper reads the Spark's
                property `spark.eventLog.dir` defined in the `cluster`.
        :param cluster: The cluster on which the Spark applications were executed. The argument
                can be a cluster name od ID (for databricks platforms) or a valid path to the cluster's
                properties file (json format) generated by the CSP SDK.
        :param platform: defines one of the following "onprem", "emr", "dataproc", "databricks-aws",
                and "databricks-azure".
        :param output_folder: path to store the output.
        :param verbose: True or False to enable verbosity of the script.
        :param rapids_options: A list of valid Profiling tool options.
                Note that the wrapper ignores ["output-directory", "worker-info"] flags, and it does not support
                multiple "spark-property" arguments.
                For more details on Profiling tool options, please visit
                https://docs.nvidia.com/spark-rapids/user-guide/latest/spark-profiling-tool.html#profiling-tool-options
        """
        if verbose:
            ToolLogging.enable_debug_mode()
        init_environment('prof')
        prof_args = AbsToolUserArgModel.create_tool_args('profiling',
                                                         eventlogs=eventlogs,
                                                         cluster=cluster,
                                                         platform=platform,
                                                         output_folder=output_folder)
        if prof_args:
            tool_obj = ProfilingAsLocal(platform_type=prof_args['runtimePlatform'],
                                        output_folder=prof_args['outputFolder'],
                                        wrapper_options=prof_args,
                                        rapids_options=rapids_options)
            tool_obj.launch()

    def bootstrap(self,
                  cluster: str,
                  platform: str,
                  output_folder: str = None,
                  dry_run: bool = True,
                  verbose: bool = False):
        """Provides optimized RAPIDS Accelerator for Apache Spark configs based on GPU cluster shape.

        This tool is supposed to be used once a cluster has been created to set the recommended
        configurations.
        The tool will apply settings for the cluster assuming that jobs will run serially so that
        each job can use up all the cluster resources (CPU and GPU) when it is running.

        :param cluster: Name or ID (for databricks platforms) of the cluster running an accelerated
                computing instance class
        :param platform: defines one of the following "onprem", "emr", "dataproc", "databricks-aws",
                and "databricks-azure".
        :param output_folder: path where the final recommendations will be saved.
        :param dry_run: True or False to update the Spark config settings on Dataproc driver node.
        :param verbose: True or False to enable verbosity of the script.
        """
        if verbose:
            ToolLogging.enable_debug_mode()
        init_environment('boot')
        boot_args = AbsToolUserArgModel.create_tool_args('bootstrap',
                                                         cluster=cluster,
                                                         platform=platform,
                                                         output_folder=output_folder,
                                                         dry_run=dry_run)
        if boot_args:
            tool_obj = Bootstrap(platform_type=boot_args['runtimePlatform'],
                                 cluster=cluster,
                                 output_folder=boot_args['outputFolder'],
                                 wrapper_options=boot_args)
            tool_obj.launch()


def main():
    # Make Python Fire not use a pager when it prints a help text
    fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
    print(gen_app_banner())
    fire.Fire(ToolsCLI())


if __name__ == '__main__':
    main()
