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
from spark_rapids_tools.enums import QualGpuClusterReshapeType
from spark_rapids_tools.utils.util import gen_app_banner, init_environment
from spark_rapids_pytools.common.utilities import Utils, ToolLogging


def run_prediction(platform: str = 'onprem',
                   qual: str = None,
                   profile: str = None,
                   output_info: str = None,
                   qualtool_filter: str = 'stage'):
    """CLI that runs prediction on the estimation_model in qualification tools."""
    print("run_prediction")

    predictions_df = predict(platform, qual, profile, output_info, qualtool_filter)


def main():
    # Make Python Fire not use a pager when it prints a help text
    fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
    print(gen_app_banner())
    fire.Fire(run_prediction)


if __name__ == '__main__':
    main()
