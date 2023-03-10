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

"""Implementation of Job submissions on EMR"""

import json
import time
from dataclasses import field, dataclass
from logging import Logger

from spark_rapids_pytools.cloud_api.sp_types import EnumeratedType
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging, Utils
from spark_rapids_pytools.rapids.rapids_job import RapidsJob, RapidsLocalJob
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


class EMRJobState(EnumeratedType):
    """
    Standard states for an EMR job.
    """
    SUBMITTED = 'submitted'
    PENDING = 'Pending'
    SCHEDULED = 'Scheduled'
    RUNNING = 'Running'
    FAILED = 'Failed'
    SUCCESS = 'Success'
    CANCELLING = 'Cancelling'
    CANCELLED = 'Cancelled'
    UNKNOWN = 'Unknown'


class EMRAppState(EnumeratedType):
    """
    Standard states for a EMR application.
    Creating 	The application is being prepared and isn't ready to use yet.
    Created 	The application has been created but hasn't provisioned capacity yet.
                 You can modify the application to change its initial capacity configuration.
    Starting 	The application is starting and is provisioning capacity.
    Started 	The application is ready to accept new jobs. The application only accepts jobs when
                it's in this state.
    Stopping 	All jobs have completed and the application is releasing its capacity.
    Stopped 	The application is stopped and no resources are running on the application.
                You can modify the application to change its initial capacity configuration.
    Terminated 	The application has been terminated and doesn't appear on your application list.
    """
    CREATING = 'Creating'
    CREATED = 'Created'
    STARTING = 'Starting'
    STARTED = 'Started'
    STOPPING = 'Stopping'
    STOPPED = 'Stopped'
    TERMINATED = 'Terminated'


class EMRAppType(EnumeratedType):
    """
    Standard types for EMR application types.
    """
    SPARK = 'SPARK'


@dataclass
class EMRServerlessApplication:
    """
    A wrapper that encapsulates an EMR-serverless application created to host the submitted
    RAPIDS tool job through EMR-serverless.
    """
    app_name: None
    id: str = None
    exec_ctxt: ToolContext = None
    app_type: EMRAppType = EMRAppType.SPARK
    state: EMRAppState = field(default_factory=dict, init=False)
    details: JSONPropertiesContainer = field(default=None, init=False)
    outlive_submission: bool = field(default=False, init=False)
    logger: Logger = field(default=None, init=False)

    def __post_init__(self):
        # when debug is set to true set it in the environment.
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.submit.app')
        if self.id is not None:
            self.outlive_submission = True
            self._update_status()
        else:
            # if name is none auto generate a new one:
            if self.app_name is None:
                self.app_name = f'rapids-tools-{Utils.gen_random_string(8)}'
            self.outlive_submission = False
            self._create_as_new()

    def _update_status(self):
        cmd_args = [
            'aws',
            'emr-serverless',
            'get-application',
            '--application-id',
            self.id
        ]
        std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
        self.details = JSONPropertiesContainer(prop_arg=std_out, file_load=False)
        self.state = EMRAppState.fromstring(self.details.get_value('application', 'state'))
        self.app_name = self.details.get_value('application', 'name')

    def _create_as_new(self):
        cmd_args = [
            'aws',
            'emr-serverless',
            'create-application',
            '--release-label',
            'emr-6.9.0',
            '--type',
            f'\"{EMRAppType.tostring(self.app_type)}\"',
            '--name',
            self.app_name
        ]
        self.logger.info('Creating new EMR-serverless application')
        std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
        json_value = json.loads(std_out)
        self.id = json_value['applicationId']
        self._update_status()
        return self

    def _wait_for_states(self, *states):
        self.logger.debug('Waiting for application to reach state: %s', states)
        while self.state not in states:
            time.sleep(5)
            self._update_status()
        self.logger.debug('Done waiting for application to reach state: %s', states)

    def app_is_terminated(self):
        return self.state in [EMRAppState.TERMINATED, EMRAppState.STOPPED]

    def wait_for_app_ready(self):
        if self.app_is_terminated():
            raise RuntimeError(f'EMR Application {self.id} is not active. '
                               f'Current state is {self.state}.')
        self._wait_for_states(EMRAppState.STARTED, EMRAppState.STARTING, EMRAppState.CREATED)

    def stop_app(self):
        self.logger.info('Start stopping application %s. Current state %s', self.id, self.state)
        if self.state not in [EMRAppState.STOPPING, EMRAppState.STOPPED, EMRAppState.TERMINATED]:
            cmd_args = [
                'aws', 'emr-serverless', 'stop-application', '--application-id', self.id
            ]
            std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
            self.logger.info('Application %s has stopped: %s', self.id, std_out)
        else:
            self.logger.info('Application %s. was already stopped %s', self.id, self.state)
        self._wait_for_states(EMRAppState.TERMINATED, EMRAppState.STOPPED)

    def delete_app(self):
        # stop app first
        self.stop_app()
        # now delete teh app
        cmd_args = [
            'aws', 'emr-serverless', 'delete-application', '--application-id', self.id
        ]
        std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
        self.logger.info('the emr-serverless application was deleted. %s', std_out)

    def terminate_app(self):
        if self.outlive_submission:
            self.logger.info('Skipping termination of Emr-serverless app %s. The app outlives the '
                             'execution.',
                             self.id)
        else:
            self.logger.info('Deleting the temporary app %s with current state %s', self.id, self.state)
            self.delete_app()


@dataclass
class EmrServerlessRapidsJob(RapidsJob):
    """
    An implementation that uses EMR-Serverless to run RAPIDS accelerator tool.
    """
    job_label = 'emrServerless'
    job_state: EMRJobState = field(default=None, init=False)
    emr_app: EMRServerlessApplication = field(default=None, init=False)

    def is_finished(self):
        if self.job_state is None:
            return False
        return self.job_state in [EMRJobState.FAILED, EMRJobState.SUCCESS, EMRJobState.CANCELLED]

    def _init_fields(self):
        super()._init_fields()
        app_id = self.prop_container.get_value_silent('platformArgs', 'application-id')
        self.emr_app = EMRServerlessApplication(id=app_id,
                                                app_name=self.__generate_app_name(),
                                                exec_ctxt=self.exec_ctxt)
        self.emr_app.wait_for_app_ready()

    def __generate_app_name(self) -> str:
        return self.exec_ctxt.get_ctxt('execFullName')

    def __get_role_arn(self):
        return self.prop_container.get_value('platformArgs', 'execution-role-arn')

    def __generate_job_name(self):
        # use the same name as the output folder
        return f'{self.emr_app.app_name}-{Utils.gen_random_string(4)}'

    def __build_driver(self) -> str:
        spark_job_configs = self.prop_container.get_value('sparkConfArgs', 'properties')
        spark_params = ['--class', self.prop_container.get_jar_main_class()]
        for conf_k, conf_val in spark_job_configs.items():
            conf_arg = ['--conf', f'{conf_k}={conf_val}']
            spark_params.extend(conf_arg)
        submit_params = {
            'entryPoint': self.prop_container.get_jar_file(),
            'entryPointArguments': self._build_rapids_args(),
            'sparkSubmitParameters': Utils.gen_joined_str(' ', spark_params)
        }
        res = {
            'sparkSubmit': submit_params
        }
        return json.dumps(res)

    def _build_submission_cmd(self):
        cmd_args = ['aws',
                    'emr-serverless',
                    'start-job-run']
        # add application_id
        cmd_args.extend(['--application-id', self.emr_app.id])
        # add job_role_arn
        cmd_args.extend(['--execution-role-arn', self.__get_role_arn()])
        # add job_name
        cmd_args.extend(['--name', self.__generate_job_name()])
        # add job_driver
        cmd_args.extend(['--job-driver', f"\'{self.__build_driver()}\'"])
        return cmd_args

    def __get_job_details(self, job_id: str, app_id: str) -> dict:
        cmd_args = [
            'aws',
            'emr-serverless',
            'get-job-run',
            '--application-id',
            app_id,
            '--job-run-id',
            job_id]
        std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
        return json.loads(std_out)

    def _submit_job(self, cmd_args: list) -> str:
        std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
        self.logger.debug('Output of job submission is %s', std_out)
        # get the job_id
        json_out = json.loads(std_out)
        job_id = json_out['jobRunId']
        self.logger.info('Submitted JOB ID is %s', job_id)
        # wait while the job is still running
        while not self.is_finished():
            time.sleep(10)
            job_details = self.__get_job_details(job_id, self.emr_app.id)
            self.job_state = EMRJobState.fromstring(job_details['jobRun']['state'])
            self.logger.info('Job state: %s', self.job_state)
        self.logger.info('Done waiting for the job to finish.')
        # Cleanup the emr application if necessary
        self.emr_app.terminate_app()
        return std_out


@dataclass
class EmrLocalRapidsJob(RapidsLocalJob):
    """
    Implementation of a RAPIDS job that runs local on a local machine.
    """
    job_label = 'emrLocal'

    def _build_submission_cmd(self) -> list:
        # env vars are added later as a separate dictionary
        cmd_arg = super()._build_submission_cmd()
        # any s3 link has to be converted to S3a:
        for index, arr_entry in enumerate(cmd_arg):
            cmd_arg[index] = arr_entry.replace('s3://', 's3a://')
        return cmd_arg
