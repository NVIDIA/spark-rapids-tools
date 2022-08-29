# Copyright (c) 2022, NVIDIA CORPORATION.
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

import fire
import sys

from spark_rapids_dataproc_tools.rapids_models import Profiling, Qualification, Bootstrap
from spark_rapids_dataproc_tools.diag import Diagnostic
from spark_rapids_dataproc_tools.diag_dataproc import DiagDataproc


class DataprocWrapper(object):
    """
    A wrapper script to run Rapids tools (Qualification, Profiling, and Bootstrap) tools on DataProc.
    Disclaimer:
      Estimates provided by the tools are based on the currently supported "SparkPlan" or
      "Executor Nodes" used in the application. It currently does not handle all the expressions
      or datatypes used.
      The pricing estimate does not take into considerations:
      1- Sustained Use discounts
      2- Cost of on-demand VMs

    Run one of the following commands:
    :qualification args
    :profiling args
    :bootstrap args

    For more details on each command: run qualification --help
    """
    def bootstrap(self,
                  cluster: str,
                  region: str,
                  dry_run: bool = False,
                  debug: bool = False) -> None:
        """
        The bootstrap tool analyzes the CPU and GPU configuration of the Dataproc cluster
        and updates the Spark default configuration on the cluster's master nodes.

        :param cluster: Name of the dataproc cluster
        :param region: Compute region (e.g. us-central1) for the cluster.
        :param dry_run: True or False to update the Spark config settings on Dataproc master node.
        :param debug: True or False to enable verbosity to the wrapper script.

        """
        boot_tool = Bootstrap(cluster=cluster,
                              region=region,
                              dry_run=dry_run,
                              debug=debug,
                              output_folder=None,
                              tools_jar=None,
                              eventlogs=None)
        boot_tool.launch()

    def profiling(self,
                  cluster: str,
                  region: str,
                  tools_jar: str = None,
                  eventlogs: str = None,
                  output_folder: str = '.',
                  debug: bool = False,
                  **tool_options) -> None:
        """
        The Profiling tool analyzes both CPU or GPU generated event logs and generates information
        which can be used for debugging and profiling Apache Spark applications.

        The output information contains the Spark version, executor details, properties, etc. It also
        uses heuristics based techniques to recommend Spark configurations for users to run Spark on RAPIDS.

        :param cluster: Name of the dataproc cluster
        :param region: Compute region (e.g. us-central1) for the cluster.
        :param tools_jar: Path to a bundled jar including Rapids tool.
                          The path is a local filesystem, or gstorage url.
        :param eventlogs: Event log filenames(comma separated) or gcloud storage directories
            containing event logs.
            eg: gs://<BUCKET>/eventlog1,gs://<BUCKET1>/eventlog2
            If not specified, the wrapper will pull the default SHS directory from the cluster
            properties, which is equivalent to gs://$temp_bucket/$uuid/spark-job-history or the
            PHS log directory if any.
        :param output_folder: Base output directory.
            The final output will go into a subdirectory called wrapper-output.
            It will overwrite any existing directory with the same name.
        :param debug: True or False to enable verbosity to the wrapper script.
        :param tool_options: A list of valid Profiling tool options.
            Note that the wrapper ignores the following flags
            [“auto-tuner“, “worker-info“, “compare“, “combined“, “output-directory“].
            For more details on Profiling tool options, please visit
            https://nvidia.github.io/spark-rapids/docs/spark-profiling-tool.html#profiling-tool-options.
        """
        prof_tool = Profiling(
            cluster=cluster,
            region=region,
            tools_jar=tools_jar,
            eventlogs=eventlogs,
            output_folder=output_folder,
            debug=debug,
            #config_path="config_path_value"
        )
        prof_tool.set_tool_options(tool_args=tool_options)
        prof_tool.launch()

    def qualification(self,
                      cluster: str,
                      region: str,
                      tools_jar: str = None,
                      eventlogs: str = None,
                      output_folder: str = '.',
                      filter_apps: str = 'savings',
                      gpu_device: str = 'T4',
                      gpu_per_machine: int = 2,
                      cuda: str = '11.5',
                      debug: bool = False,
                      **tool_options) -> None:
        """
        The Qualification tool analyzes Spark events generated from CPU based Spark applications to
        help quantify the expected acceleration and costs savings of migrating a Spark application
        or query to GPU.

        Disclaimer:
            Estimates provided by the Qualification tool are based on the currently supported "SparkPlan" or
            "Executor Nodes" used in the application.
            It currently does not handle all the expressions or datatypes used.
            Please refer to "Understanding Execs report" section and the "Supported Operators" guide
            to check the types and expressions you are using are supported.

        :param cluster: Name of the dataproc cluster
        :param region: Compute region (e.g. us-central1) for the cluster.
        :param tools_jar: Path to a bundled jar including Rapids tool.
                          The path is a local filesystem, or gstorage url.
        :param eventlogs: Event log filenames(comma separated) or gcloud storage directories
            containing event logs.
            eg: gs://<BUCKET>/eventlog1,gs://<BUCKET1>/eventlog2
            If not specified, the wrapper will pull the default SHS directory from the cluster
            properties, which is equivalent to gs://$temp_bucket/$uuid/spark-job-history or the
            PHS log directory if any.
        :param output_folder: Base output directory.
            The final output will go into a subdirectory called wrapper-output.
            It will overwrite any existing directory with the same name.
        :param filter_apps: [NONE | recommended | savings] filtering criteria of the applications
            listed in the final STDOUT table. Note that this filter does not affect the CSV report.
            “NONE“ means no filter applied. “recommended“ lists all the apps that are either
            'Recommended', or 'Strongly Recommended'. “savings“ lists all the apps that have positive
            estimated GPU savings.
        :param gpu_device: The type of the GPU to add to the cluster. Options are [T4, V100, K80, A100, P100].
        :param gpu_per_machine: The number of GPU accelerators to be added to each VM image.
        :param cuda: cuda version to be used with the GPU accelerator.
        :param debug: True or False to enable verbosity to the wrapper script.
        :param tool_options: A list of valid Qualification tool options.
            Note that the wrapper ignores the “output-directory“ flag, and it does not support
            multiple “spark-property“ arguments.
            For more details on Qualification tool options, please visit
            https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html#qualification-tool-options.
        """
        qualification_tool = Qualification(
            cluster=cluster,
            region=region,
            tools_jar=tools_jar,
            eventlogs=eventlogs,
            output_folder=output_folder,
            filter_apps=filter_apps,
            gpu_device=gpu_device,
            gpu_per_machine=gpu_per_machine,
            cuda=cuda,
            debug=debug,
            #config_path="config_path_value"
        )
        qualification_tool.set_tool_options(tool_args=tool_options)
        qualification_tool.launch()

    def diagnostic(self,
                       cluster: str,
                       region: str,
                       func: str = 'all',
                       debug: bool = False) -> None:
        """
        Run diagnostic on local environment or remote Dataproc cluster, such as check installed NVIDIA driver,
        CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.

        :param cluster: Name of the Dataproc cluster
        :param region: Region of Dataproc cluster (e.g. us-central1)
        :param func: Diagnostic function to run. Available functions:
            'nv_driver': dump NVIDIA driver info via command `nvidia-smi`,
            'cuda_version': check if CUDA toolkit major version >= 11.0,
            'rapids_jar': check if only single RAPIDS Accelerator for Apache Spark jar is installed and verify its signature,
            'deprecated_jar': check if deprecated (cudf) jar is installed. I.e. should no cudf jar starting with RAPIDS
                    Accelerator for Apache Spark 22.08,
            'spark': run a Hello-world Spark Application on CPU and GPU,
            'perf': performance test for a Spark job between CPU and GPU,
            'spark_job': run a Hello-world Spark Application on CPU and GPU via Dataproc job interface,
            'perf_job': performance test for a Spark job between CPU and GPU via Dataproc job interface
        :param debug: True or False to enable verbosity
        """
        if not cluster or not region:
            raise Exception('Invalid cluster or region for Dataproc environment. '
                            'Please provide options "--cluster=<CLUSTER_NAME> --region=<REGION>" properly.')

        diag = DiagDataproc(cluster, region, debug)

        # Run diagnostic function
        getattr(diag, func)()

        # Dump summary & check result
        if not diag.print_summary():
            sys.exit(1)


def main():
    fire.Fire(DataprocWrapper)


if __name__ == '__main__':
    main()
