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

import glob
import logging.config
import os
import re
import sys
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Optional, List, Dict
from urllib.request import urlopen

import pandas as pd
import yaml
from tabulate import tabulate

import spark_rapids_dataproc_tools.bootstrap as srdt_bootstrap
from spark_rapids_dataproc_tools.cost_estimator import DataprocCatalogContainer, DataprocPriceProvider, \
    DataprocSavingsEstimator
from spark_rapids_dataproc_tools.dataproc_utils import validate_dataproc_sdk, get_default_region, \
    validate_region, CMDRunner, DataprocClusterPropContainer
from spark_rapids_dataproc_tools.utilities import bail, \
    get_log_dict, remove_dir, make_dirs, resource_path, YAMLPropertiesContainer


@dataclass
class ToolContext(YAMLPropertiesContainer):
    name: str = 'rapids_tool'
    debug: bool = False
    logger: Logger = field(default=None, init=False)
    cli: CMDRunner = field(default=None, init=False)

    def _init_fields(self):
        log_arg = {'debug': self.debug}
        logging.config.dictConfig(get_log_dict(log_arg))
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)
        self.props['localCtx'] = dict()
        self.props['remoteCtx'] = dict()
        self.cli = CMDRunner(self.logger, self.debug)

    def set_fail_actions(self, method):
        self.cli.fail_action = method

    def loginfo(self, msg: str):
        self.logger.info(msg)

    def logdebug(self, msg: str):
        self.logger.debug(msg)

    def logwarn(self, msg: str):
        self.logger.warning(msg)

    def set_remote(self, key: str, val: str):
        self.props['remoteCtx'][key] = val

    def set_local(self, key: str, val: Any):
        self.props['localCtx'][key] = val

    def get_local(self, key: str):
        return self.props['localCtx'].get(key)

    def get_remote(self, key: str):
        return self.props['remoteCtx'].get(key)

    def set_local_workdir(self, parent: str):
        relative_path = self.get_value('platform', 'workDir')
        local_work_dir = os.path.join(parent, relative_path)
        self.logdebug(f'creating dependency folder {local_work_dir}')
        # first delete the folder if it exists
        remove_dir(local_work_dir, fail_on_error=False)
        # now create the new folder
        make_dirs(local_work_dir, exist_ok=False)
        self.set_local('depFolder', local_work_dir)
        output_folder = os.path.join(local_work_dir, self.get_value('platform', 'outputDir'))
        self.set_local('toolOutputFolder', output_folder)
        test_val = self.get_local('toolOutputFolder')
        self.logdebug(f'setting local output folder of the tool to {test_val}')

    def get_remote_output_dir(self) -> str:
        remote_work_dir = self.get_remote('depFolder')
        return os.path.join(remote_work_dir, self.get_value('platform', 'outputDir'))

    def get_local_output_dir(self) -> str:
        local_folder = self.get_wrapper_local_output()
        if self.get_value_silent('toolOutput', 'subFolder') is None:
            return local_folder
        return os.path.join(local_folder, self.get_value('toolOutput', 'subFolder'))

    def get_wrapper_local_output(self) -> str:
        local_folder = os.path.join(self.get_local_work_dir(), self.get_value('platform', 'outputDir'))
        return local_folder

    def set_remote_workdir(self, parent: str):
        remote_work_dir = os.path.join(parent, self.get_value('platform', 'workDir'))
        self.set_remote('depFolder', remote_work_dir)

    def get_local_work_dir(self) -> str:
        return self.get_local('depFolder')

    def get_remote_work_dir(self) -> str:
        return self.get_remote('depFolder')

    def get_default_jar_name(self) -> str:
        jar_version = self.get_value('sparkRapids', 'version')
        default_jar_name = self.get_value('sparkRapids', 'jarFile')
        return default_jar_name.format(jar_version)

    def get_rapids_jar_url(self) -> str:
        jar_version = self.get_value('sparkRapids', 'version')
        rapids_url = self.get_value('sparkRapids', 'repoUrl').format(jar_version, jar_version)
        return rapids_url

    def get_tool_main_class(self) -> str:
        return self.get_value('sparkRapids', 'mainClass')


@dataclass
class RapidsTool(object):
    cluster: str
    region: str
    output_folder: str
    tools_jar: str
    eventlogs: str
    debug: bool
    config_path: str = None
    tool_options: dict = field(default_factory=dict)
    name: str = field(default=None, init=False)
    ctxt: ToolContext = field(default=None, init=False)
    dataproc_props: DataprocClusterPropContainer = field(default=None, init=False)

    def set_tool_options(self, tool_args: Dict[str, Any]) -> None:
        """
        Sets the options that will be passed to the RAPIDS Tool.
        :param tool_args: key value pair of the arguments passed from CLI
        :return: NONE
        """
        for key, value in tool_args.items():
            if not isinstance(value, bool):
                # a boolean flag, does not need to have its value added to the list
                if isinstance(value, str):
                    # if the argument is multiple word, then protect it with single quotes.
                    if re.search(r"\s|\(|\)|,", value):
                        value = f"'{value}'"
                self.tool_options.setdefault(key, []).append(value)
            else:
                if value:
                    self.tool_options.setdefault(key, [])
                else:
                    # argument parser removes the "no-" prefix and set the value to false.
                    # we need to restore the original key
                    self.tool_options.setdefault(f"no{key}", [])

    def accept_tool_option(self, option_key: str) -> bool:
        defined_tool_options = self.ctxt.get_value_silent('sparkRapids', 'cli', 'tool_options')
        if defined_tool_options is not None:
            if option_key not in defined_tool_options:
                self.ctxt.logwarn(f"Ignoring tool option '{option_key}'. Invalid option.")
                return False
        return True

    def process_tool_options(self) -> List[str]:
        """
        Process the arguments passed from the CLI if any and retuen a string representing the
        arguments to be passed to the final command running the job.
        :return: a string of space separated key value pair
        """
        arguments_list = []
        for key, value in self.tool_options.items():
            self.ctxt.logdebug(f"Processing tool CLI argument.. {key}:{value}")
            if len(key) > 1:
                # python forces "_" to "-". we need to reverse that back.
                fixed_key = key.replace("_", "-")
                prefix = "--"
            else:
                # shortcut argument
                fixed_key = key
                prefix = "-"
            if self.accept_tool_option(fixed_key):
                k_arg = f"{prefix}{fixed_key}"
                if len(value) >= 1:
                    # handle list options
                    for value_entry in value[0:]:
                        arguments_list.append(f"{k_arg}")
                        arguments_list.append(f"{value_entry}")
                else:
                    # this could be a boolean type flag that has no arguments
                    arguments_list.append(f"{k_arg}")
        return arguments_list

    def get_wrapper_arguments(self, arg_list: List[str]) -> List[str]:
        res = arg_list
        res.extend([
            "--output-directory",
            f"{self.ctxt.get_remote_output_dir()}",
            f" {self.ctxt.get_remote('eventlogs')}"])
        return res

    def generate_final_tool_arguments(self, arg_list: List[str]) -> str:
        wrapper_arguments = self.get_wrapper_arguments(arg_list)
        tool_argument_arr = self.process_tool_options()
        tool_argument_arr.extend(wrapper_arguments)
        return " ".join(tool_argument_arr)

    def _report_tool_full_location(self) -> str:
        out_folder_path = os.path.abspath(self.ctxt.get_local_output_dir())
        res_arr = [f"{self.name.capitalize()} tool output is saved to local disk {out_folder_path}"]
        subfiles = glob.glob(f'{out_folder_path}/*', recursive=False)
        if len(subfiles) > 0:
            res_arr.append(f"\t{os.path.basename(out_folder_path)}/")
            for sub_file in subfiles:
                if os.path.isdir(sub_file):
                    leaf_name = f"└── {os.path.basename(sub_file)}/"
                else:
                    leaf_name = f"├── {os.path.basename(sub_file)}"
                res_arr.append(f"\t\t{leaf_name}")
            doc_url = self.ctxt.get_value('sparkRapids', 'outputDocURL')
            res_arr.append(f"- To learn more about the output details, visit "
                           f"{doc_url}")
            return "\n".join(res_arr)

    def _check_environment(self) -> None:
        self.ctxt.logdebug("Checking Environment has requirements installed correctly")
        validate_dataproc_sdk()

    def _process_region_arg(self):
        self.ctxt.logdebug("Checking Region is set correctly")
        if self.region is None:
            # get the dataproc region from the system environment
            self.region = get_default_region()
            validate_region(self.region)

    def _init_ctxt(self):
        if self.config_path is None:
            self.config_path = resource_path("{}-conf.yaml".format(self.name))
        self.ctxt = ToolContext(prop_arg=self.config_path, name=self.name, debug=self.debug)
        self.ctxt.set_fail_actions(self.terminate)
        self.ctxt.logdebug(f'config_path = {self.config_path}')

    def __post_init__(self):
        pass

    def _pull_cluster_properties(self) -> str:
        cluster_describe = f'dataproc clusters describe {self.cluster} --region={self.region}'
        fail_msg = f'Could not pull Cluster description region:{self.region}, {self.cluster}'
        res = self.ctxt.cli.gcloud(cluster_describe,
                                   msg_fail=fail_msg)
        return res

    def _init_cluster_dataproc_props(self):
        self.ctxt.logdebug("Initializing Dataproc Properties")
        raw_props = self._pull_cluster_properties()
        self.dataproc_props = DataprocClusterPropContainer(prop_arg=raw_props,
                                                           file_load=False,
                                                           cli=self.ctxt.cli)

    def _process_output_arg(self):
        self.ctxt.logdebug("Processing Output Arguments")
        workdir = os.path.join(self.output_folder, 'wrapper-output')
        self.ctxt.set_local_workdir(workdir)

    def _process_jar_arg(self):
        if self.tools_jar is None:
            # we should download the jar file
            local_jar_path = os.path.join(self.ctxt.get_local_work_dir(), self.ctxt.get_default_jar_name())
            wget_cmd = 'wget -O "{}" "{}"'.format(local_jar_path, self.ctxt.get_rapids_jar_url())
            self.ctxt.cli.run(wget_cmd,
                              msg_fail='Failed downloading tools jar url')
            jar_file_name = self.ctxt.get_default_jar_name()
        else:
            # copy the tools_jar to the dependency folder
            if self.tools_jar.startswith('gs://'):
                # this is a gstorage_path
                # use gsutil command to get it on local disk first
                self.ctxt.logdebug(f'Downloading the toolsJar {self.tools_jar} to local disk')
                self.ctxt.cli.gcloud_cp(self.tools_jar,
                                        self.ctxt.get_local_work_dir(),
                                        is_dir=False)
                jar_file_name = self.tools_jar.rsplit('/', 1)[-1]
            else:
                # this is a local disk
                copy_jar_cmd = 'cp "{}" "{}"'.format(self.tools_jar, self.ctxt.get_local_work_dir())
                self.ctxt.cli.run(copy_jar_cmd, msg_fail='Failed to copy the Jar tools to the dep folder')
                jar_file_name = os.path.basename(self.tools_jar)
        self.ctxt.logdebug(f'the toolsJar fileName: {jar_file_name}')
        self.ctxt.set_remote('jarFileName', jar_file_name)

    def _process_event_logs(self):
        logs_dir = self.eventlogs
        if self.eventlogs is None:
            # find the default event logs
            logs_dir = self.dataproc_props.get_default_hs_dir()
        if isinstance(logs_dir, tuple):
            processed_logs = List[logs_dir]
        elif isinstance(logs_dir, str):
            processed_logs = logs_dir.split(",")
        else:
            processed_logs = logs_dir
        self.ctxt.set_remote('eventlogs', " ".join(processed_logs))
        logs = self.ctxt.get_remote('eventlogs')
        self.ctxt.logdebug(f'Eventlogs are set to {logs}')

    def _prepare_dependencies(self):
        """
        this function is responsible to prepare the dependency folder by:
        1- pull cluster properties because this contains information on staging environment
        2- getting the jar file
        3- creating any input file needed to run the tool
        :return:
        """
        self._init_cluster_dataproc_props()
        self._process_jar_arg()
        self._process_event_logs()

    def _prepare_remote_env(self):
        self.ctxt.loginfo("Preparing remote work env")
        # set the staging directory
        self.ctxt.set_remote_workdir(self.dataproc_props.get_temp_gs_storage())
        self.ctxt.logdebug(f"cleaning up the remote work dir if it exists {self.ctxt.get_remote_work_dir()}")
        self.ctxt.cli.gcloud_rm(self.ctxt.get_remote_work_dir(), fail_ok=True)

    def _run_tool_as_spark(self):
        self.ctxt.loginfo("Running the tool as a spark job on dataproc")

    def _download_tool_output(self):
        self.ctxt.loginfo("Downloading the tool output")
        # download cmd. it is possible that the cmd fail if the tool did not generate an output
        self.ctxt.cli.gcloud_cp(self.ctxt.get_remote_output_dir(),
                                self.ctxt.get_local_work_dir(),
                                fail_ok=True)

    def _report_results_are_empty(self) -> None:
        print(f'The {self.name.capitalize()} tool did not generate any output. Nothing to display.')

    def _post_remote_run(self):
        self._download_tool_output()
        # check if the file exist
        output_dir = self.ctxt.get_local_output_dir()
        if os.path.exists(output_dir):
            self._process_tool_output()
        else:
            self._report_results_are_empty()

    def _upload_dependencies(self):
        self.ctxt.loginfo("Upload dependencies to remote cluster")
        self.ctxt.logdebug(f'Uploading {self.ctxt.get_local_work_dir()} to {self.ctxt.get_remote_work_dir()}')
        self.ctxt.cli.gcloud_cp(self.ctxt.get_local_work_dir(), self.ctxt.get_remote_work_dir())

    def _process_tool_output(self):
        self.ctxt.loginfo("Processing tool output")

    def _process_custom_args(self):
        pass

    def _init_tool(self):
        self._init_ctxt()
        self._check_environment()
        self._process_region_arg()
        self._process_output_arg()
        self._process_custom_args()

    def _initialize_remote_env(self):
        self._prepare_dependencies()
        self._prepare_remote_env()
        self._upload_dependencies()

    def dump_str(self) -> str:
        return f'this is the {self.name} tool running {self.config_path}'

    def _local_cleanup(self, run_fail: bool) -> None:
        # cleanup local directory
        if self.ctxt.get_local_work_dir() is not None:
            if run_fail or not os.path.exists(self.ctxt.get_wrapper_local_output()):
                # when this is failed run, remove the entire workdir
                remove_dir(self.ctxt.get_local_work_dir(), fail_on_error=False)
            else:
                sub_items = glob.glob(f'{self.ctxt.get_local_work_dir()}/*', recursive=False)
                output_dir = self.ctxt.get_wrapper_local_output()
                for child_path in sub_items:
                    try:
                        if output_dir != child_path:
                            if os.path.isdir(child_path):
                                remove_dir(child_path)
                            else:
                                os.remove(child_path)
                    except OSError:
                        self.ctxt.logwarn('Failed to cleanup remote data')

    def _remote_cleanup(self) -> None:
        if self.ctxt.get_remote_work_dir() is not None:
            self.ctxt.cli.gcloud_rm(self.ctxt.get_remote_work_dir(), fail_ok=True)

    def _cleanup(self, run_fail: bool = False):
        self.ctxt.logdebug("Cleaning up after finishing the tool execution")
        # cleanup remote
        local_clean_enabled = self.ctxt.get_value('local', 'output', 'cleanUp')
        remote_clean_enabled = self.ctxt.get_value('platform', 'cleanUp')
        if remote_clean_enabled:
            self._remote_cleanup()
        if local_clean_enabled:
            self._local_cleanup(run_fail)

    def terminate(self, err=None, msg=Optional[str]):
        self._cleanup(run_fail=err is not None)
        if err is not None:
            error_msg = (
                f'Failure Running Rapids Tool.\n\t'
                f'{msg}\n\t'
                f'Run Terminated with error.\n\t'
                f'{err}'
            )
            print(error_msg)
            sys.exit(1)

    def launch(self):
        self._init_tool()
        self._initialize_remote_env()
        self._run_tool_as_spark()
        self._post_remote_run()
        self.terminate()


@dataclass
class Profiling(RapidsTool):
    name = 'profiling'

    def dump_str(self) -> str:
        return f'this is the {self.name} tool running {self.config_path}'

    def __generate_autotuner_input(self):
        self.ctxt.logdebug(f'generating input files for Auto-tuner')
        cluster_info = self.dataproc_props.convert_props_to_dict()
        cluster_info_path = os.path.join(self.ctxt.get_local_work_dir(),
                                         'dataproc_worker_info.yaml')
        with open(cluster_info_path, 'w') as worker_info_file:
            self.ctxt.logdebug(f'Auto-tuner worker info file {cluster_info_path}')
            self.ctxt.logdebug(f'Auto-tuner worker info: {cluster_info}')
            yaml.dump(cluster_info, worker_info_file, sort_keys=False)
            self.ctxt.set_remote('autoTunerFileName', 'dataproc_worker_info.yaml')

    def _prepare_dependencies(self):
        super()._prepare_dependencies()
        self.__generate_autotuner_input()

    def __read_single_app_output(self, file_path: str) -> (List[str], List[str], str):
        def __split_list_str_by_pattern(input_seq: List[str], pattern: str) -> int:
            ind = 0
            while ind < len(input_seq):
                if input_seq[ind].find(pattern) != -1:
                    return ind
                ind += 1
            return -1

        try:
            props_list = []
            comments_list = []
            app_name: str = ""
            with open(file_path, 'rt') as app_profiler:
                raw_lines = [line.strip() for line in app_profiler.readlines() if line.strip()]
                # find the app_name
                app_name_candidates = re.findall("(\|spark\.app\.name\s+\|)(.+)\|",
                                                 "\n".join(raw_lines),
                                                 flags=re.MULTILINE)
                if len(app_name_candidates) > 0:
                    grp_1, grp_2 = app_name_candidates[0]
                    app_name = grp_2.strip()
                header_pattern = self.ctxt.get_value('toolOutput', 'recommendations', 'headers',
                                                     'section')
                spark_pattern = self.ctxt.get_value('toolOutput', 'recommendations', 'headers',
                                                    'sparkProperties')
                comments_pattern = self.ctxt.get_value('toolOutput', 'recommendations', 'headers',
                                                       'comments')
                begin_props_ind = -1
                last_props_ind = -1
                begin_comm_ind = -1
                last_comm_ind = -1
                section_ind = __split_list_str_by_pattern(raw_lines, header_pattern)
                if section_ind != -1:
                    recom_section = raw_lines[section_ind:]
                    recom_properties_ind = __split_list_str_by_pattern(recom_section,
                                                                       spark_pattern)
                    if recom_properties_ind != -1 and recom_properties_ind != len(recom_section) - 1:
                        begin_props_ind = recom_properties_ind + 1
                    recom_comments_ind = __split_list_str_by_pattern(recom_section, comments_pattern)
                    if recom_comments_ind != -1:
                        last_props_ind = recom_comments_ind
                        begin_comm_ind = recom_comments_ind + 1
                        last_comm_ind = len(recom_section)
                    else:
                        last_props_ind = len(recom_section)
                        last_comm_ind = len(recom_section)
                if begin_props_ind != -1:
                    props_list = recom_section[begin_props_ind: last_props_ind]
                if begin_comm_ind != -1:
                    comments_list = recom_section[begin_comm_ind: last_comm_ind]
        except OSError:
            print(f'could not open output of profiler {file_path}')
        if len(props_list) == 0:
            props_list = ["- No recommendations"]
        if len(comments_list) == 0:
            comments_list = ["- No comments"]
        return props_list, comments_list, app_name

    def _process_tool_output(self):
        super()._process_tool_output()
        app_folders = glob.glob(f'{self.ctxt.get_local_output_dir()}/*', recursive=False)
        wrapper_content = []
        if len(app_folders) == 0:
            curr_line = f'The {self.name.capitalize()} tool did not generate any output. Nothing to display.'
            print(curr_line)
            wrapper_content.append(curr_line)
        else:
            # loop on all the application folders
            print(self._report_tool_full_location())
            recommendations_table = []
            header_str = f'### Recommended configurations ###'
            print(header_str)
            wrapper_content.append(header_str)
            headers = self.ctxt.get_value('local', 'output', 'summaryColumns')
            for app_folder in app_folders:
                if os.path.isdir(app_folder):
                    app_id = os.path.basename(app_folder)
                    profile_file = self.ctxt.get_value('toolOutput', 'recommendations', 'fileName')
                    recommendations, comments, app_name = self.__read_single_app_output(f'{app_folder}/{profile_file}')
                    row = [app_id, app_name, "\n".join(recommendations), "\n".join(comments)]
                    wrapper_content.append(app_id)
                    wrapper_content.append("\t{}".format("\n\t".join(recommendations)))
                    wrapper_content.append("\t{}".format("\n\t".join(comments)))
                    recommendations_table.append(row)
            print(tabulate(recommendations_table, headers, tablefmt="grid"))
        output_file_name = self.ctxt.get_value('local', 'output', 'fileName')
        wrapper_output_file = os.path.join(self.ctxt.get_local_output_dir(), output_file_name)
        with open(wrapper_output_file, 'w') as wrapper_output:
            wrapper_output.write("\n".join(wrapper_content))

    def _run_tool_as_spark(self):
        super()._run_tool_as_spark()
        # set the arguments
        jars_path = os.path.join(self.ctxt.get_remote_work_dir(),
                                 self.ctxt.get_remote('jarFileName'))
        worker_info_path = os.path.join(self.ctxt.get_remote_work_dir(),
                                        self.ctxt.get_remote('autoTunerFileName'))

        tool_arguments = self.generate_final_tool_arguments(["--auto-tuner",
                                                             "--worker-info",
                                                             f"{worker_info_path}"])

        submit_cmd = (
            f"dataproc jobs submit spark --cluster={self.cluster}"
            f" --region={self.region}"
            f" --jars={jars_path}"
            f" --class={self.ctxt.get_tool_main_class()}"
            f" --"
            f" {tool_arguments}"
        )
        self.ctxt.logdebug(f'Going to submit job {submit_cmd}')
        self.ctxt.cli.gcloud(submit_cmd, msg_fail='Failed Submitting Spark job')


@dataclass
class QualificationSummary:
    comments: Any = None
    all_apps: pd.DataFrame = None
    recommended_apps: pd.DataFrame = None
    df_result: pd.DataFrame = None

    def _get_total_durations(self) -> int:
        if not self.is_empty():
            return self.all_apps["App Duration"].sum()
        return 0

    def _get_total_gpu_durations(self) -> int:
        if not self.is_empty():
            return self.all_apps["Estimated GPU Duration"].sum()
        return 0

    def _get_stats_total_cost(self) -> float:
        return self.df_result["Estimated App Cost"].sum()

    def _get_stats_total_gpu_cost(self) -> float:
        return self.df_result["Estimated GPU Cost"].sum()

    def _get_stats_total_apps(self) -> int:
        if not self.is_empty():
            return len(self.all_apps)
        return 0

    def _get_stats_recommended_apps(self) -> int:
        if self.has_gpu_recommendation():
            return len(self.recommended_apps)
        return 0

    def is_empty(self) -> bool:
        if self.all_apps is not None:
            return self.all_apps.empty
        return True

    def has_gpu_recommendation(self) -> bool:
        if self.recommended_apps is not None:
            return not self.recommended_apps.empty
        return False

    def has_tabular_result(self) -> bool:
        if self.df_result is not None:
            return not self.df_result.empty
        return False

    def print_report(self,
                     app_name: str,
                     wrapper_csv_file: str = None,
                     config_provider=None,
                     df_pprinter: Any = None,
                     output_pprinter: Any = None) -> None:

        def format_float(x: float) -> str:
            return "{:.2f}".format(x)

        report_content = []

        if self.is_empty():
            # Qualification tool has no output
            print(f'{app_name} tool did not generate any valid rows')
            if self.comments is not None and len(self.comments) > 0:
                print('\n'.join(self.comments))
            return None

        if output_pprinter is not None:
            report_content.append(output_pprinter())

        if not self.has_gpu_recommendation():
            report_content.append(f'{app_name} tool found no recommendations for GPU.')

        if self.has_tabular_result():
            if wrapper_csv_file is not None:
                abs_path = os.path.abspath(wrapper_csv_file)
                report_content.append(f"Full savings and speedups CSV report: {abs_path}")

            pretty_df = df_pprinter(self.df_result)
            if pretty_df.empty:
                # the results were reduced to no rows because of the filters
                report_content.append(
                    f'{app_name} tool found no qualified applications after applying the filters.\n'
                    f'See the CSV file for full report or disable the filters.')
            else:
                report_content.append(tabulate(pretty_df, headers='keys', tablefmt='psql', floatfmt='.2f'))
        else:
            report_content.append(f"{app_name} tool found no records to show.")

        total_app_cost = self._get_stats_total_cost()
        total_gpu_cost = self._get_stats_total_gpu_cost()
        estimated_gpu_savings = 0.0
        if total_app_cost > 0.0:
            estimated_gpu_savings = 100.0 - (100.0 * total_gpu_cost / total_app_cost)
        overall_speedup = 0.0
        total_apps_durations = 1.0 * self._get_total_durations()
        total_gpu_durations = self._get_total_gpu_durations()
        if total_gpu_durations > 0:
            overall_speedup = total_apps_durations / total_gpu_durations
        report_content.append("Report Summary:")
        report_summary = [["Total applications", self._get_stats_total_apps()],
                          ["RAPIDS candidates", self._get_stats_recommended_apps()],
                          ["Overall estimated speedup", format_float(overall_speedup)],
                          ["Overall estimated cost savings", f"{format_float(estimated_gpu_savings)}%"]]
        report_content.append(tabulate(report_summary, colalign=("left", "right")))
        if self.comments is not None and len(self.comments) > 0:
            report_content.extend(self.comments)
        if self.has_gpu_recommendation() and config_provider is not None:
            report_content.append(config_provider())

        print('\n'.join(report_content))


@dataclass
class Qualification(RapidsTool):
    name = 'qualification'
    filter_apps: str = None
    gpu_device: str = None
    gpu_per_machine: int = None
    cuda: str = None

    def dump_str(self) -> str:
        return f'this is the {self.name} tool running {self.config_path}'

    def __write_cluster_properties(self):
        self.ctxt.set_local('cluster_props',
                            os.path.join(self.ctxt.get_local_work_dir(), "cluster_props.yaml"))
        self.dataproc_props.write_as_yaml_file(self.ctxt.get_local('cluster_props'))

    def _process_custom_args(self):
        def process_filter_opt(arg_val: str):
            available_filters = self.ctxt.get_value('sparkRapids', 'cli', 'defaults', 'filters',
                                                    'definedFilters')
            default_filter = self.ctxt.get_value('sparkRapids', 'cli', 'defaults', 'filters',
                                                 'defaultFilter')
            if arg_val is None:
                self.filter_apps = default_filter
            else:
                selected_filter = arg_val.lower().strip()
                if selected_filter in available_filters:
                    # correct argument
                    self.filter_apps = selected_filter
                else:
                    self.ctxt.logwarn(
                        f"Invalid argument filter_apps={selected_filter}.\n\t"
                        f"Accepted options are: [{' | '.join(available_filters)}].\n\t"
                        f"Falling-back to default filter: {default_filter}"
                    )
                    self.filter_apps = default_filter

        if self.gpu_device is None:
            self.gpu_device = self.ctxt.get_value('sparkRapids', 'gpu', 'device')
        if self.gpu_per_machine is None:
            self.gpu_per_machine = int(self.ctxt.get_value('sparkRapids', 'gpu', 'workersPerNode'))
        if self.cuda is None:
            self.cuda = self.ctxt.get_value('sparkRapids', 'gpu', 'cudaVersion')
        process_filter_opt(self.filter_apps)

    def __generate_qualification_configs(self) -> str:
        initialization_actions = self.ctxt.get_value('sparkRapids',
                                                     'gpu',
                                                     'initializationScripts').format(self.region, self.region)
        instructions_str = (
            f'To launch a GPU-accelerated cluster with RAPIDS Accelerator for Apache Spark, add the '
            f'following to your cluster creation script:\n\t'
            f'--initialization-actions={initialization_actions} \\ \n\t'
            f'--worker-accelerator type=nvidia-tesla-{self.gpu_device.lower()},'
            f'count={self.gpu_per_machine} \\ \n\t'
            f'--metadata gpu-driver-provider="NVIDIA" \\ \n\t'
            f'--metadata rapids-runtime=SPARK \\ \n\t'
            f'--cuda-version={self.cuda}')
        return instructions_str

    def _report_results_are_empty(self) -> None:
        super()._report_results_are_empty()
        # check if we should report unsupported
        try:
            worker_machine, converted = self.dataproc_props.convert_worker_machine_if_not_supported()
            if converted:
                print(f"To support acceleration with T4 GPUs, you will need to switch "
                      f"your worker node instance type to {worker_machine}")
        except Exception as e:
            self.ctxt.logdebug("Exception converting worker machine type {}".format(e))

    def _process_tool_output(self):
        def get_costs_for_single_app(df_row,
                                     estimator: DataprocSavingsEstimator) -> pd.Series:
            est_cpu_cost, est_gpu_cost, est_savings = estimator.get_costs_and_savings(df_row['App Duration'],
                                                                                      df_row['Estimated GPU Duration'])
            return pd.Series([est_cpu_cost, est_gpu_cost, est_savings])

        def get_recommended_apps(all_rows, selected_cols) -> pd.DataFrame:
            speed_up_col = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport',
                                               'recommendations', 'speedUp', 'columnName')
            recommended_vals = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport',
                                                   'recommendations', 'speedUp', 'selectedRecommendations')
            mask = all_rows[speed_up_col].isin(recommended_vals)
            return all_rows.loc[mask, selected_cols]

        def process_df_for_stdout(raw_df):
            """
            process the dataframe to be more readable on the stdout
            1- convert time durations to second
            2- shorten headers
            """
            selected_cols = self.ctxt.get_value('local', 'output', 'summaryColumns')
            # check if any filters apply
            filter_recom_enabled = (self.filter_apps == 'recommended')
            filter_pos_enabled = (self.filter_apps == 'savings')
            # filter by recommendations if enabled
            if filter_recom_enabled:
                df_row = get_recommended_apps(raw_df, selected_cols)
            else:
                df_row = raw_df.loc[:, selected_cols]
            if df_row.empty:
                return df_row
            # filter by savings if enabled
            if filter_pos_enabled:
                cost_col = self.ctxt.get_value('local', 'output', 'savingColumn')
                cost_mask = df_row[cost_col] > 0.0
                df_row = df_row.loc[cost_mask, selected_cols]
                if df_row.empty:
                    print("Found no qualified apps for cost savings.")
                    return df_row
            time_unit = '(ms)'
            time_from_conf = self.ctxt.get_value('toolOutput', 'stdout', 'summaryReport', 'timeUnits')
            if time_from_conf == 's':
                time_unit = '(s)'
                # convert to seconds
                for column in df_row[[col for col in df_row.columns if 'Duration' in col]]:
                    df_row[column] = df_row[column].div(1000).round(2)
            # change the header to include time unit
            df_row.columns = df_row.columns.str.replace('Duration',
                                                        f'Duration{time_unit}', regex=False)
            # squeeze the header titles if enabled
            if self.ctxt.get_value('toolOutput', 'stdout', 'summaryReport', 'compactWidth'):
                for column in df_row.columns:
                    if len(column) > 10:
                        split_ch = ' '
                        split_pos = column.rfind(split_ch)
                        if split_pos > -1:
                            new_column_name = column[:split_pos] + '\n' + column[split_pos + len(split_ch):]
                            df_row.columns = df_row.columns.str.replace(column,
                                                                        new_column_name, regex=False)
            return df_row

        def create_savings_estimator() -> DataprocSavingsEstimator:
            """
            Construct the savings estimator object after initializing its metadata.
            :return: the dataproc savings estimator.
            """
            dataproc_catalog: DataprocCatalogContainer = None
            load_from_url = self.ctxt.get_value('local', 'costCalculation', 'catalog', 'loadFromURLEnabled')
            load_from_snapshot = not load_from_url
            if load_from_url:
                # load catalog from url
                url_address = self.ctxt.get_value('local', 'costCalculation', 'catalog', 'onlineURL')
                try:
                    self.ctxt.loginfo(f"Downloading the price catalog from URL {url_address}")
                    response = urlopen(url_address)
                    dataproc_catalog = DataprocCatalogContainer(prop_arg=response.read(), file_load=False)
                    self.ctxt.logdebug(f"Successful download of cloud pricing catalog")
                except Exception as url_ex:
                    # failed to load the catalog from url, then revert to snapshot file
                    load_from_snapshot = True
                    self.ctxt.logwarn(f"Failed to download the cloud pricing catalog with error {url_ex}."
                                      f"\n\tFalling back to snapshot file.")
            if load_from_snapshot:
                # load catalog from snapshot_file because either url has failed or it is disabled
                snapshot_file = self.ctxt.get_value('local', 'costCalculation', 'catalog', 'snapshotFile')
                self.ctxt.loginfo(f"Loading price catalog from snapshot file {snapshot_file}")
                dataproc_catalog = DataprocCatalogContainer(resource_path(snapshot_file))
            price_provider = DataprocPriceProvider(name="dataprocCostEstimator",
                                                   catalog=dataproc_catalog)
            savings_estimator = DataprocSavingsEstimator(price_provider=price_provider)
            savings_estimator.setup_calculations(self.dataproc_props)
            return savings_estimator

        def build_global_report_summary(
                all_apps: pd.DataFrame,
                csv_out: str) -> QualificationSummary:
            # initialize the savings estimator
            savings_estimator = create_savings_estimator()
            if all_apps.empty:
                return QualificationSummary(comments=savings_estimator.comments)
            cols = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'columns')
            cost_cols = self.ctxt.get_value('local', 'output', 'costColumns')
            recommended_apps = get_recommended_apps(all_apps, cols)

            # pick the apps to be used to generate the apps
            apps_working_set = all_apps.loc[:, cols]
            apps_working_set[cost_cols] = apps_working_set.apply(
                lambda row: get_costs_for_single_app(row, estimator=savings_estimator), axis=1)
            if not apps_working_set.empty:
                self.ctxt.loginfo(
                    f'Generating GPU Estimated Speedup and Savings as {csv_out}')
                apps_working_set.to_csv(csv_out)
            return QualificationSummary(comments=savings_estimator.comments,
                                        all_apps=all_apps,
                                        recommended_apps=recommended_apps,
                                        df_result=apps_working_set)

        super()._process_tool_output()
        try:
            self.__write_cluster_properties()
        except Exception as ex:
            file_name = self.ctxt.get_local('cluster_props')
            bail(f'Could not Save the cluster properties as yaml file {file_name}', ex)

        summary_file = os.path.join(self.ctxt.get_local_output_dir(),
                                    self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'fileName'))
        self.ctxt.logdebug(f'The local CSV file is {summary_file}')
        try:
            df = pd.read_csv(summary_file)
            csv_file_name = self.ctxt.get_value('local', 'output', 'fileName')
            local_csv = os.path.join(self.ctxt.get_wrapper_local_output(), csv_file_name)
            report_summary = build_global_report_summary(df, local_csv)
            report_summary.print_report(app_name=self.name.capitalize(),
                                        wrapper_csv_file=local_csv,
                                        config_provider=self.__generate_qualification_configs,
                                        df_pprinter=process_df_for_stdout,
                                        output_pprinter=self._report_tool_full_location)
        except Exception as ex:
            bail('Error Parsing CSV file to generate Row Cost', ex)

    def _run_tool_as_spark(self):
        super()._run_tool_as_spark()
        # set the arguments
        jars_path = os.path.join(self.ctxt.get_remote_work_dir(), self.ctxt.get_remote('jarFileName'))
        tool_arguments = self.generate_final_tool_arguments([])
        submit_cmd = (
            f"dataproc jobs submit spark --cluster={self.cluster}"
            f" --region={self.region}"
            f" --jars={jars_path}"
            f" --class={self.ctxt.get_tool_main_class()}"
            f" --"
            f" {tool_arguments}"
        )
        self.ctxt.logdebug(f'Going to submit job {submit_cmd}')
        self.ctxt.cli.gcloud(submit_cmd, msg_fail='Failed Submitting Spark job')


@dataclass
class Bootstrap(RapidsTool):
    name = 'bootstrap'
    dry_run: bool = False

    def launch(self):
        validate_dataproc_sdk()
        validate_region(self.region)
        cluster_config = srdt_bootstrap.get_cluster_config(self.cluster, self.region)
        spark_settings = srdt_bootstrap.get_bootstrap_configs(self.cluster, cluster_config)
        print(spark_settings)
        if not self.dry_run:
            srdt_bootstrap.update_driver_nodes(cluster_config, spark_settings)
