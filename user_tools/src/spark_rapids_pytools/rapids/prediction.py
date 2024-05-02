# Copyright (c) 2024, NVIDIA CORPORATION.
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

"""Implementation class representing wrapper around the RAPIDS acceleration Prediction tool."""

import os

from dataclasses import dataclass

from spark_rapids_tools.tools.model_xgboost import predict, _print_summary, _print_speedup_summary
from spark_rapids_pytools.cloud_api.sp_types import get_platform
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_tool import RapidsTool
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


@dataclass
class Prediction(RapidsTool):
    """
    Wrapper layer around Prediction Tool.
    """
    qual_output: str = None
    prof_output: str = None

    name = 'prediction'

    def _check_environment(self):
        pass

    def _connect_to_execution_cluster(self):
        pass

    def _init_ctxt(self):
        # reuse qualification configs for its predictionModel section
        self.config_path = Utils.resource_path('qualification-conf.yaml')
        self.ctxt = ToolContext(platform_cls=get_platform(self.platform_type),
                                platform_opts=self.wrapper_options.get('platformOpts'),
                                prop_arg=self.config_path,
                                name=self.name)

    def _process_output_args(self):
        self.logger.debug('Processing Output Arguments')
        if self.output_folder is None:
            self.output_folder = os.getcwd()
        self.output_folder = FSUtil.get_abs_path(self.output_folder)
        exec_dir_name = f'{self.name}_{self.ctxt.uuid}'
        # It should never happen that the exec_dir_name exists
        self.output_folder = FSUtil.build_path(self.output_folder, exec_dir_name)
        FSUtil.make_dirs(self.output_folder, exist_ok=False)
        self.ctxt.set_local('outputFolder', self.output_folder)
        self.logger.info('Local output folder is set as: %s', self.output_folder)

    def prepare_prediction_output_info(self):
        # build the full output path for the predictions output files
        predictions_info = self.ctxt.get_value('local', 'output', 'predictionModel')
        output_dir = FSUtil.build_path(self.ctxt.get_output_folder(), predictions_info['outputDirectory'])
        FSUtil.make_dirs(output_dir)

        files_info = predictions_info['files']
        # update files_info dictionary with full file paths
        for entry in files_info:
            file_name = files_info[entry]['name']
            file_path = FSUtil.build_path(output_dir, file_name)
            files_info[entry]['path'] = file_path
        return files_info

    def _run_rapids_tool(self):
        output_info = self.prepare_prediction_output_info()
        df = predict(self.platform_type.map_to_java_arg(), self.qual_output, self.prof_output, output_info)
        _print_summary(df)
        _print_speedup_summary(df)
        df.to_csv(f'{self.output_folder}/prediction.csv', float_format='%.2f')

    def _collect_result(self):
        pass

    def _archive_phase(self):
        pass

    def _finalize(self):
        pass
