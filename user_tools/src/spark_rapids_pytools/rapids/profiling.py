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

"""Implementation class representing wrapper around the RAPIDS acceleration Profiling tool."""

import re
from dataclasses import dataclass
from itertools import chain
from typing import List

import yaml
from tabulate import tabulate

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_job import RapidsJobPropContainer
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
        self._process_gpu_cluster_args(offline_cluster_opts)
        self._generate_autotuner_input()

    def _process_gpu_cluster_args(self, offline_cluster_opts: dict = None):
        gpu_cluster_arg = offline_cluster_opts.get('gpuCluster')
        if gpu_cluster_arg:
            gpu_cluster_obj = self._create_migration_cluster('GPU', gpu_cluster_arg)
            self.ctxt.set_ctxt('gpuClusterProxy', gpu_cluster_obj)
        else:
            # If we are here, we know that the workerInfoPath was not set as well.
            # Then we should fail
            self.logger.error('The gpuCluster argument was not set. '
                              'Please make sure to set the arguments properly by either:\n'
                              '  1. Setting <gpu_cluster> argument and optional set <eventlogs> if '
                              'the path is not defined by the cluster properties ; or\n'
                              '  2. Setting both <worker_info> and <eventlogs>')
            raise RuntimeError('Invalid Arguments: The <gpu_cluster> and <worker_info> arguments are '
                               'not defined. Aborting Execution.')

    def _generate_autotuner_file_for_cluster(self, file_path: str, cluster_ob: ClusterBase):
        """
        Given file path and the cluster object, it will generate the formatted input file in yaml
        that can be used by the autotuner to run the profiling tool.
        :param file_path: local path whether the file should be stored
        :param cluster_ob: the object representing the cluster proxy.
        :return:
        """
        self.logger.info('Generating input file for Auto-tuner')
        worker_hw_info = cluster_ob.get_worker_hw_info()
        worker_info = {
            'system': {
                'numCores': worker_hw_info.sys_info.num_cpus,
                'memory': f'{worker_hw_info.sys_info.cpu_mem}MiB',
                'numWorkers': cluster_ob.get_workers_count()
            },
            'gpu': {
                # the scala code expects a unit
                'memory': f'{worker_hw_info.gpu_info.gpu_mem}MiB',
                'count': worker_hw_info.gpu_info.num_gpus,
                'name': worker_hw_info.gpu_info.get_gpu_device_name()
            },
            'softwareProperties': cluster_ob.get_all_spark_properties()
        }
        self.logger.debug('Auto-tuner worker info: %s', worker_info)
        with open(file_path, 'w', encoding='utf-8') as worker_info_file:
            self.logger.debug('Opening file %s to write worker info', file_path)
            yaml.dump(worker_info, worker_info_file, sort_keys=False)

    def _generate_autotuner_input(self):
        gpu_cluster_obj = self.ctxt.get_ctxt('gpuClusterProxy')
        input_file_name = 'worker_info.yaml'
        self.ctxt.set_ctxt('autoTunerFileName', input_file_name)
        autotuner_input_path = FSUtil.build_path(self.ctxt.get_local_work_dir(), 'worker_info.yaml')
        self._generate_autotuner_file_for_cluster(file_path=autotuner_input_path,
                                                  cluster_ob=gpu_cluster_obj)
        self.logger.info('Generated autotuner worker info: %s', autotuner_input_path)
        self.ctxt.set_ctxt('autoTunerFilePath', autotuner_input_path)

    def _create_autotuner_rapids_args(self) -> list:
        # add the autotuner argument
        autotuner_args = ['--auto-tuner',
                          '--worker-info',
                          self.ctxt.get_ctxt('autoTunerFilePath')]
        return autotuner_args

    def __read_single_app_output(self, file_path: str) -> (List[str], List[str], str):
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
                                                 '\n'.join(raw_lines),
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
            print(f'could not open output of profiler {file_path}')
        if len(props_list) == 0:
            props_list = ['- No recommendations']
        if len(comments_list) == 0:
            comments_list = ['- No comments']
        return props_list, comments_list, app_name

    def _write_summary(self):
        wrapper_summary = [
            self._report_tool_full_location(),
            self.ctxt.get_ctxt('wrapperOutputContent')
        ]
        print('\n'.join(wrapper_summary))

    def _process_output(self):
        rapids_output_dir = self.ctxt.get_rapids_output_folder()
        if not self.ctxt.platform.storage.resource_exists(rapids_output_dir):
            self.ctxt.set_ctxt('wrapperOutputContent',
                               self._report_results_are_empty())
            return
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
            recommendations, comments, app_name = self.__read_single_app_output(f'{app_folder}/{profiling_log}')
            row = [app_id, app_name, '\n'.join(recommendations), '\n'.join(comments)]
            log_lines.append(app_id)
            sec_props = '\n\t'.join(list(chain(sec_props_head, recommendations)))
            sec_comments = '\n\t'.join(list(chain(sec_comments_head, comments)))
            log_lines.append(f'{sec_props}')
            log_lines.append(f'{sec_comments}')
            recommendations_table.append(row)
        log_file_name = self.ctxt.get_value('local', 'output', 'fileName')
        summary_file = FSUtil.build_path(self.ctxt.get_output_folder(), log_file_name)
        self.logger.info('Writing recommendations into local file %s', summary_file)
        with open(summary_file, 'w', encoding='utf-8') as wrapper_summary:
            wrapper_summary.write('\n'.join(log_lines))
        self.logger.info('Generating Full STDOUT summary report')
        # wrapper STDOUT report contains both tabular and plain text format of recommendations
        wrapper_content = [Utils.gen_str_header('Recommendations'),
                           '\n'.join(log_lines),
                           '### Recommendations Table Summary ###',
                           tabulate(recommendations_table, headers, tablefmt='grid')]
        self.ctxt.set_ctxt('wrapperOutputContent', '\n'.join(wrapper_content))


@dataclass
class ProfilingAsLocal(Profiling):
    """
    Profiling tool running on local development.
    """
    description: str = 'This is the localProfiling'

    def _process_job_submission_args(self):
        job_args = {}
        submission_args = self.wrapper_options.get('jobSubmissionProps')
        # get the root remote folder and make sure it exists
        remote_folder = submission_args.get('remoteFolder')
        # If remote_folder is not specified, then ignore it
        if remote_folder is None:
            # the output is only for local machine
            self.logger.info('No remote output folder specified.')
        else:
            # TODO verify the remote folder is correct
            if not self.ctxt.platform.storage.resource_exists(remote_folder):
                raise RuntimeError(f'Remote folder [{remote_folder}] is invalid.')
            # now we should make the subdirectory to indicate the output folder,
            # by appending the name of the execution folder
            exec_full_name = self.ctxt.get_ctxt('execFullName')
            remote_workdir = FSUtil.build_url_from_parts(remote_folder, exec_full_name)
            self.ctxt.set_remote('rootFolder', remote_folder)
            self.ctxt.set_remote('workDir', remote_workdir)
            self.logger.info('Remote workdir is set as %s', remote_workdir)
            remote_dep_folder = FSUtil.build_url_from_parts(remote_workdir,
                                                            self.ctxt.get_ctxt('depFolderName'))
            self.ctxt.set_remote('depFolder', remote_dep_folder)
            self.logger.info('Remote dependency folder is set as %s', remote_dep_folder)
        # the output folder has to be set any way
        job_args['outputFolder'] = self.ctxt.get_output_folder()
        platform_args = submission_args.get('platformArgs')
        if platform_args is not None:
            processed_platform_args = self.ctxt.platform.cli.build_local_job_arguments(platform_args)
            ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
            dependencies = ctxt_rapids_args.get('javaDependencies')
            processed_platform_args.update({'dependencies': dependencies})
            job_args['platformArgs'] = processed_platform_args
        self.ctxt.update_job_args(job_args)

    def _prepare_job_arguments(self):
        job_args = self.ctxt.get_ctxt('jobArgs')
        output_folder = job_args.get('outputFolder')
        # now we can create the job object
        # Todo: For dataproc, this can be autogenerated from cluster name
        rapids_arg_list = self._create_autotuner_rapids_args()
        ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
        jar_file_path = ctxt_rapids_args.get('jarFilePath')
        rapids_opts = ctxt_rapids_args.get('rapidsOpts')
        if rapids_opts:
            rapids_arg_list.extend(rapids_opts)
        # add the eventlogs at the end of all the tool options
        rapids_arg_list.extend(self.ctxt.get_ctxt('eventLogs'))
        class_name = self.ctxt.get_value('sparkRapids', 'mainClass')
        rapids_arg_obj = {
            'jarFile': jar_file_path,
            'jarArgs': rapids_arg_list,
            'className': class_name
        }
        platform_args = job_args.get('platformArgs')
        spark_conf_args = {}
        job_properties_json = {
            'remoteOutput': output_folder,
            'rapidsArgs': rapids_arg_obj,
            'sparkConfArgs': spark_conf_args,
            'platformArgs': platform_args
        }
        job_properties = RapidsJobPropContainer(prop_arg=job_properties_json,
                                                file_load=False)
        job_obj = self.ctxt.platform.create_local_submission_job(job_prop=job_properties,
                                                                 ctxt=self.ctxt)
        job_obj.run_job()

    def _get_main_cluster_obj(self):
        return self.ctxt.get_ctxt('gpuClusterProxy')

    def _download_remote_output_folder(self):
        self.logger.debug('Local mode skipping downloading the remote output workdir')

    def _archive_results(self):
        remote_work_dir = self.ctxt.get_remote('workDir')
        if remote_work_dir is not None:
            local_folder = self.ctxt.get_output_folder()
            # TODO make sure it worth issuing the command
            self.ctxt.platform.storage.upload_resource(local_folder, remote_work_dir)

    def _delete_remote_dep_folder(self):
        self.logger.debug('Local mode skipping deleting the remote workdir')
