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

"""Implementation class representing wrapper around the RAPIDS acceleration Profiling tool."""

import re
from dataclasses import dataclass
from itertools import chain
from typing import List

from tabulate import tabulate

from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_tool import RapidsJarTool


@dataclass
class Profiling(RapidsJarTool):
    """
    Wrapper layer around Profiling Tool.
    """
    name = 'profiling'

    def _process_worker_info_arg(self):
        worker_info_arg = self.wrapper_options.get('autoTunerFileInput')
        if not worker_info_arg:
            return
        self.logger.info('Processing WorkerInfo argument [%s]', worker_info_arg)
        # download the worker_file into the work dir
        input_path = self.ctxt.platform.storage.download_resource(worker_info_arg,
                                                                  self.ctxt.get_local_work_dir(),
                                                                  fail_ok=False,
                                                                  create_dir=True)
        self.ctxt.set_ctxt('autoTunerFilePath', input_path)
        self.ctxt.set_ctxt('autoTunerFileName', FSUtil.get_resource_name(input_path))
        self.logger.info('WorkerInfo successfully processed into workDir [%s]', input_path)

    def _process_custom_args(self):
        """
        Profiling tool processes extra arguments:
        1. the worker_info argument
        2. the clusters
        """
        self._process_worker_info_arg()
        # if the workerInfo is not set, then we need to use the gpu_cluster
        if not self.ctxt.get_ctxt('autoTunerFilePath'):
            self._process_offline_cluster_args()
        else:
            self.logger.info('Skipping building of GPU_CLUSTER because WorkerInfo is defined')
        self._process_eventlogs_args()

    def _process_offline_cluster_args(self):
        offline_cluster_opts = self.wrapper_options.get('migrationClustersProps', {})
        if self._process_gpu_cluster_args(offline_cluster_opts):
            # only if we succeed to get the GPU cluster, we can generate auto-tuner-input
            self._generate_autotuner_input()

    def _process_gpu_cluster_args(self, offline_cluster_opts: dict = None):
        gpu_cluster_arg = offline_cluster_opts.get('gpuCluster')
        if gpu_cluster_arg:
            gpu_cluster_obj = self._create_migration_cluster('GPU', gpu_cluster_arg)
            self.ctxt.set_ctxt('gpuClusterProxy', gpu_cluster_obj)
            return True
        # If we are here, we know that the workerInfoPath was not set as well.
        return False

    def _generate_autotuner_input(self):
        gpu_cluster_obj = self.ctxt.get_ctxt('gpuClusterProxy')
        self._generate_autotuner_input_from_cluster(gpu_cluster_obj)

    def __read_single_app_output(self, file_path: str) -> (str, List[str], List[str]):
        def split_list_str_by_pattern(input_seq: List[str], pattern: str) -> int:
            ind = 0
            while ind < len(input_seq):
                if input_seq[ind].find(pattern) != -1:
                    return ind
                ind += 1
            return -1

        try:
            props_list = []
            comments_list = []
            app_name: str = ''
            with open(file_path, 'rt', encoding='utf-8') as app_profiler:
                raw_lines = [line.strip() for line in app_profiler.readlines() if line.strip()]
                # find the app_name
                app_name_candidates = re.findall(r'(\|spark\.app\.name\s+\|)(.+)\|',
                                                 Utils.gen_multiline_str(raw_lines),
                                                 flags=re.MULTILINE)
                if len(app_name_candidates) > 0:
                    _, grp_2 = app_name_candidates[0]
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
                section_ind = split_list_str_by_pattern(raw_lines, header_pattern)
                if section_ind != -1:
                    recom_section = raw_lines[section_ind:]
                    recom_properties_ind = split_list_str_by_pattern(recom_section,
                                                                     spark_pattern)
                    if recom_properties_ind not in (-1, len(recom_section) - 1):
                        begin_props_ind = recom_properties_ind + 1
                    recom_comments_ind = split_list_str_by_pattern(recom_section, comments_pattern)
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
            self.logger.error('Could not open output of profiler %s', file_path)
        if len(props_list) == 0:
            props_list = ['- No recommendations']
        if len(comments_list) == 0:
            comments_list = ['- No comments']
        # Note that sorting the comments is disabled because it will change the order
        # of multiline entries
        # Recommendations can be sorted so that the two values are aligned
        # comments_list.sort()
        props_list.sort()
        return app_name, props_list, comments_list

    def _write_summary(self):
        print(Utils.gen_multiline_str(self._report_tool_full_location(),
                                      self.ctxt.get_ctxt('wrapperOutputContent')))

    def __generate_report_with_recommendations(self):
        prof_app_dirs = FSUtil.get_subdirectories(self.ctxt.get_rapids_output_folder())
        profiling_log = self.ctxt.get_value('toolOutput', 'recommendations', 'fileName')
        recommendations_table = []
        log_lines = []

        header_str = '### Recommended configurations ###'
        sec_props_head = ['\tSpark Properties:']
        sec_comments_head = ['\tComments:']
        log_lines.append(header_str)
        headers = self.ctxt.get_value('local', 'output', 'summaryColumns')
        for app_folder in prof_app_dirs:
            app_id = FSUtil.get_resource_name(app_folder)
            app_name, recommendations, comments = self.__read_single_app_output(f'{app_folder}/{profiling_log}')
            row = [app_id,
                   app_name,
                   Utils.gen_multiline_str(recommendations),
                   Utils.gen_multiline_str(comments)]
            log_lines.append(app_id)
            sec_props = Utils.gen_joined_str(join_elem='\n\t',
                                             items=list(chain(sec_props_head, recommendations)))
            sec_comments = Utils.gen_joined_str(join_elem='\n\t',
                                                items=list(chain(sec_comments_head, comments)))
            log_lines.append(f'{sec_props}')
            log_lines.append(f'{sec_comments}')
            recommendations_table.append(row)
        log_file_name = self.ctxt.get_value('local', 'output', 'fileName')
        summary_file = FSUtil.build_path(self.ctxt.get_output_folder(), log_file_name)
        self.logger.info('Writing recommendations into local file %s', summary_file)
        log_file_lines_str = Utils.gen_multiline_str(log_lines)
        with open(summary_file, 'w', encoding='utf-8') as wrapper_summary:
            wrapper_summary.write(log_file_lines_str)
        self.logger.info('Generating Full STDOUT summary report')
        # wrapper STDOUT report contains both tabular and plain text format of recommendations
        wrapper_content = [Utils.gen_report_sec_header('Recommendations'),
                           log_file_lines_str,
                           '### Recommendations Table Summary ###',
                           tabulate(recommendations_table, headers, tablefmt='grid')]
        self.ctxt.set_ctxt('wrapperOutputContent', wrapper_content)

    def _process_output(self):
        if not self._evaluate_rapids_jar_tool_output_exist():
            return

        self.__generate_report_with_recommendations()

    def _init_rapids_arg_list(self) -> List[str]:
        rapids_threads_args = self._get_rapids_threads_count(self.name) + ['--csv']
        return super()._init_rapids_arg_list() + self._create_autotuner_rapids_args() + rapids_threads_args


@dataclass
class ProfilingAsLocal(Profiling):
    """
    Profiling tool running on local development.
    """
    description: str = 'This is the localProfiling'

    def _get_main_cluster_obj(self):
        return self.ctxt.get_ctxt('gpuClusterProxy')

    def _download_remote_output_folder(self):
        self.logger.debug('Local mode skipping downloading the remote output workdir')

    def _delete_remote_dep_folder(self):
        self.logger.debug('Local mode skipping deleting the remote workdir')
