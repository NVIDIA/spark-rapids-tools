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
"""Wrapper class to run tools associated with RAPIDS Accelerator for Apache Spark plugin."""

import sys

import fire

from spark_rapids_dataproc_tools.diag_dataproc import DiagDataproc
from spark_rapids_dataproc_tools.rapids_models import Profiling, Qualification, Bootstrap
from spark_rapids_dataproc_tools.data_validation_dataproc import DataValidationDataproc

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
                  output_folder: str = '.',
                  dry_run: bool = False,
                  debug: bool = False) -> None:
        """
        The bootstrap tool analyzes the CPU and GPU configuration of the Dataproc cluster
        and updates the Spark default configuration on the cluster's master nodes.

        :param cluster: Name of the dataproc cluster
        :param region: Compute region (e.g. us-central1) for the cluster.
        :param dry_run: True or False to update the Spark config settings on Dataproc master node.
        :param output_folder: Base output directory. The final recommendations will be logged in the
               subdirectory 'wrapper-output/rapids_user_tools_bootstrap'.
               Note that this argument only accepts local filesystem.
        :param debug: True or False to enable verbosity to the wrapper script.

        """
        boot_tool = Bootstrap(cluster=cluster,
                              region=region,
                              dry_run=dry_run,
                              debug=debug,
                              output_folder=output_folder,
                              tools_jar=None,
                              eventlogs=None)
        boot_tool.launch()

    def profiling(self,
                  cluster: str,
                  region: str,
                  tools_jar: str = None,
                  eventlogs: str = None,
                  output_folder: str = '.',
                  gpu_cluster_props: str = None,
                  gpu_cluster_region: str = None,
                  gpu_cluster_zone: str = None,
                  debug: bool = False,
                  **tool_options) -> None:
        """
        The Profiling tool analyzes both CPU or GPU generated event logs and generates information
        which can be used for debugging and profiling Apache Spark applications.

        The output information contains the Spark version, executor details, properties, etc. It also
        uses heuristics based techniques to recommend Spark configurations for users to run Spark on RAPIDS.

        :param cluster: Name of the dataproc cluster.
               Note that the cluster has to: 1- be running; and 2- support Spark3.x+.
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
        :param gpu_cluster_props: Path to a file containing configurations of the GPU cluster
            on which the Spark applications ran on.
            The path is a local filesystem, or gstorage url.
            This option does not require the cluster to be live. When missing, the configurations
            are pulled from the live cluster on which the Qualification tool is submitted.
        :param gpu_cluster_region: The region where the GPU cluster belongs to. Note that this parameter requires
            'gpu_cluster_props' to be defined.
            When missing, the region is set to the value passed in the 'region' argument.
        :param gpu_cluster_zone: The zone where the GPU cluster belongs to. Note that this parameter requires
            'gpu_cluster_props' to be defined.
            When missing, the zone is set to the same zone as the 'cluster' on which the Profiling tool is
            submitted.
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
            migration_clusters_props={
                'gpu_cluster_props_path': gpu_cluster_props,
                'gpu_cluster_region': gpu_cluster_region,
                'gpu_cluster_zone': gpu_cluster_zone,
            },
            debug=debug,
            # config_path="config_path_value"
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
                      cpu_cluster_props: str = None,
                      cpu_cluster_region: str = None,
                      cpu_cluster_zone: str = None,
                      gpu_cluster_props: str = None,
                      gpu_cluster_region: str = None,
                      gpu_cluster_zone: str = None,
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

        :param cluster: Name of the dataproc cluster on which the Qualification tool is executed.
               Note that the cluster has to: 1- be running; and 2- support Spark3.x+.
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
        :param cpu_cluster_props: Path to a file containing configurations of the CPU cluster
            on which the Spark applications were executed.
            The path is a local filesystem, or gstorage url.
            This option does not require the cluster to be live.
            When missing, the configurations are pulled from the live cluster on which the Qualification tool
            is submitted.
        :param cpu_cluster_region: The region where the CPU cluster belongs to. Note that this parameter requires
            'cpu_cluster_props' to be defined.
            When missing, the region is set to the value passed in the 'region' argument.
        :param cpu_cluster_zone: The zone where the CPU cluster belongs to. Note that this parameter requires
            'cpu_cluster_props' to be defined.
            When missing, the zone is set to the same zone as the 'cluster' on which the Qualification tool is
            submitted.
        :param gpu_cluster_props: Path to a file containing configurations of the GPU cluster
            on which the Spark applications is planned to be migrated.
            The path is a local filesystem, or gstorage url.
            This option does not require the cluster to be live. When missing, the configurations are
            considered the same as the ones used by the 'cpu_cluster_props'.
        :param gpu_cluster_region: The region where the GPU cluster belongs to. Note that this parameter requires
            'gpu_cluster_props' to be defined.
            When missing, the region is set to the value passed in the 'region' argument.
        :param gpu_cluster_zone: The zone where the GPU cluster belongs to. Note that this parameter requires
            'gpu_cluster_props' to be defined.
            When missing, the zone is set to the same zone as the 'cluster' on which the Qualification tool is
            submitted.
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
            migration_clusters_props={
                'cpu_cluster_props_path': cpu_cluster_props,
                'cpu_cluster_region': cpu_cluster_region,
                'cpu_cluster_zone': cpu_cluster_zone,
                'gpu_cluster_props_path': gpu_cluster_props,
                'gpu_cluster_region': gpu_cluster_region,
                'gpu_cluster_zone': gpu_cluster_zone,
            },
            cuda=cuda,
            debug=debug,
            # config_path="config_path_value"
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

        :param cluster: Name of the Dataproc cluster.
        :param region: Region of Dataproc cluster (e.g. us-central1)
        :param func: Diagnostic function to run. Available functions:
            'nv_driver': dump NVIDIA driver info via command `nvidia-smi`,
            'cuda_version': check if CUDA toolkit major version >= 11.0,
            'rapids_jar': check if only single RAPIDS Accelerator for Apache Spark jar is installed
               and verify its signature,
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


    def validation(self,
                   cluster: str,
                   region: str,
                   check: str = 'all',
                   format: str = None,
                   t1: str = None,
                   t1p: str = None,
                   t2: str = None,
                   t2p: str = None,
                   pk: str = None,
                   e: str = None,
                   i: str = 'all',
                   f: str = None,
                   o: str = None,
                   of: str = 'parquet',
                   p: int = 4,
                   debug: bool = False) -> None:
        """
        Run diagnostic on local environment or remote Dataproc cluster, such as check installed NVIDIA driver,
        CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.

        Run data validation tool on remote Dataproc cluster to compare whether two tables have same results, one scenario is it will be easier for
        users to determine whether the Spark job using RAPIDS Accelerator(aka GPU Spark job)
        returns the same result as the CPU Spark job. Here we assume two tables have same column names.

        :param cluster: Name of the Dataproc cluster.
        :param region: Region of Dataproc cluster (e.g. us-central1)
        :param check: Metadata validation or Data validation (e.g. --check metadata or –check data. default is to run both metadata and data validation.)

        :param format: The format of tables, if the format is parquet/orc/csv, the t1 and t2 should be an absolute path. Options are [hive, orc, parquet, csv](e.g. --format hive or --format parquet)
        :param t1: The first table name, if the format is parquet/orc/csv, this value should be an absolute path. (e.g. --t1 table1)
        :param t1p: The first table’s partition clause. (e.g. --t1p 'partition1=p1 and partition2=p2')
        :param t2: The second table name, if the format is parquet/orc/csv, this value should be an absolute path.. (e.g. --t2 table2)
        :param t2p: The second table’s partition clause. (e.g. --t2p 'partition1=p1 and partition2=p2')
        :param pk: The Primary key columns(comma separated). (e.g. --pk pk1,pk2,pk3)
        :param e: Exclude column option. What columns do not need to be involved in the comparison, default is None. (e.g. --e col4,col5,col6)
        :param i: Include column option. What columns need to be involved in the comparison, default is ALL. (e.g. --i col1,col2,col3)
        :param f: Condition to filter rows. (e.g. --f “col1>value1 and col2 <> value2”)
        :param o: Output directory, the tool will generate a data file to a path. (e.g. --o /data/output)
        :param of: Output format, default is parquet. (e.g. --of parquet)
        :param p: Precision, if it is set to 4 digits, then 0.11113 == 0.11114 will return true for numeric columns. (e.g. -p 4)
        :param debug: True or False to enable verbosity

        """

        if not cluster or not region:
            raise Exception('Invalid cluster or region for Dataproc environment. '
                            'Please provide options "--cluster=<CLUSTER_NAME> --region=<REGION>" properly.')

        print('----dataproc wrapper------')
        print(f)
        validate = DataValidationDataproc(cluster, region, check, format, t1, t1p, t2, t2p, pk, e, i, f, o, of, p, debug)
        getattr(validate, check)()

def main():
    fire.Fire(DataprocWrapper)


if __name__ == '__main__':
    main()