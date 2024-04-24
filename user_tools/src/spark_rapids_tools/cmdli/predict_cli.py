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

"""CLI to run prediction on estimation_ model in qualification tools associated with RAPIDS Accelerator for Apache Spark plugin."""


import fire

from spark_rapids_tools.cmdli.argprocessor import AbsToolUserArgModel
from spark_rapids_tools.tools.model_xgboost import predict
from spark_rapids_tools.utils.util import gen_app_banner
from spark_rapids_pytools.rapids.qualification import QualificationAsLocal

def run_prediction(result_folder: str,
                   platform: str = 'onprem'):
    """CLI that runs prediction on the estimation_model in qualification tools.

    The prediction CLI takes existing qualification and profiling tool output and runs
    prediction of the GPU speedups on the estimation_model in the qualification tools.

    :param result_folder: path to the qualification and profiling tool output
    :param platform: defines one of the following "onprem", "emr", "dataproc", "dataproc-gke",
           "databricks-aws", and "databricks-azure", default to "onprem".
    """

    qual_args = AbsToolUserArgModel.create_tool_args('qualification',
                                                     eventlogs="",
                                                     platform=platform,
                                                     output_folder=result_folder)

    tool_obj = QualificationAsLocal(platform_type=qual_args['runtimePlatform'],
                                    output_folder=qual_args['outputFolder'],
                                    wrapper_options=qual_args)
    tool_obj._init_ctxt()
    tool_obj.ctxt.set_local('outputFolder', result_folder)
    output_info = tool_obj._Qualification__build_prediction_output_files_info()
    predict(platform, result_folder, result_folder, output_info)


def main():
    # Make Python Fire not use a pager when it prints a help text
    fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
    print(gen_app_banner())
    fire.Fire(run_prediction)


if __name__ == '__main__':
    main()
