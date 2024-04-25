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

"""CLI to run prediction on estimation_ model in qualification tools."""


import fire

from spark_rapids_tools.cmdli.argprocessor import AbsToolUserArgModel
from spark_rapids_tools.tools.model_xgboost import predict
from spark_rapids_tools.utils.util import gen_app_banner
from spark_rapids_pytools.rapids.prediction import Prediction


def run_prediction(result_folder: str,
                   platform: str = 'onprem'):
    """The prediction cmd takes existing qualification and profiling tool output and runs the
    estimation model in the qualification tools for GPU speedups.

    :param result_folder: path to the qualification and profiling tool output.
    :param platform: defines one of the following "onprem", "emr", "dataproc","dataproc-gke",
           "databricks-aws", and "databricks-azure", default to "onprem".
    """

    predict_args = AbsToolUserArgModel.create_tool_args('prediction',
                                                        platform=platform,
                                                        result_folder=result_folder)

    tool_obj = Prediction(platform_type=predict_args['runtimePlatform'],
                          result_folder=predict_args['resultFolder'],
                          wrapper_options=predict_args)
    tool_obj.init_ctxt()
    output_info = tool_obj.prepare_prediction_output_info()
    predict(platform, result_folder, result_folder, output_info)


def main():
    # Make Python Fire not use a pager when it prints a help text
    fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
    print(gen_app_banner())
    fire.Fire(run_prediction)


if __name__ == '__main__':
    main()
