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

"""CLI to run development related tools."""


import fire

from spark_rapids_tools.cmdli.argprocessor import AbsToolUserArgModel
from spark_rapids_tools.enums import CspEnv
from spark_rapids_tools.utils.util import gen_app_banner, init_environment
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.dev.instance_description import InstanceDescription
from spark_rapids_pytools.rapids.qualification_stats import SparkQualStats


class DevCLI(object):  # pylint: disable=too-few-public-methods
    """CLI to run development related tools (for internal use only)."""

    def generate_instance_description(self,
                                      platform: str = None,
                                      output_folder: str = None):
        """The generate_instance_description cmd takes a platform and generates a json file with all the
        instance type descriptions for that CSP platform.

        :param platform: defines one of the following "dataproc", "emr", and "databricks-azure".
        :param output_folder: local path to store the output.
        """
        # Since this is an internal tool, we enable debug mode by default
        ToolLogging.enable_debug_mode()

        init_environment('generate_instance_description')

        instance_description_args = AbsToolUserArgModel.create_tool_args('generate_instance_description',
                                                                         cli_class='DevCLI',
                                                                         cli_name='spark_rapids_dev',
                                                                         target_platform=platform,
                                                                         output_folder=output_folder)
        if instance_description_args:
            tool_obj = InstanceDescription(platform_type=instance_description_args['targetPlatform'],
                                           output_folder=instance_description_args['output_folder'],
                                           wrapper_options=instance_description_args)
            tool_obj.launch()

    def stats(self,
              config_path: str = None,
              output_folder: str = None,
              qual_output: str = None):
        """The stats cmd generates statistics from the qualification tool output.

        Statistics is generated per AppId, per SQLID, and per Operator. For each operator, the
        statistics include the number of times the operator was executed, total task time of
        the stages that contain the operator, the total task time of the SQL that the operator
        is part of. The count of operators is also differentiated whether the operator is
        supported or unsupported.

        :param config_path: Path to the configuration file.
        :param output_folder: Path to store the output.
        :param qual_output: path to the directory, which contains the qualification tool output.
                            E.g. user should specify the parent directory $WORK_DIR where
                            $WORK_DIR/rapids_4_spark_qualification_output exists.
        """
        ToolLogging.enable_debug_mode()
        init_environment('stats')

        stats_args = AbsToolUserArgModel.create_tool_args('stats',
                                                          platform=CspEnv.get_default(),
                                                          config_path=config_path,
                                                          output_folder=output_folder,
                                                          qual_output=qual_output)
        tool_obj = SparkQualStats(platform_type=stats_args['runtimePlatform'],
                                  config_path=config_path,
                                  output_folder=output_folder,
                                  qual_output=qual_output,
                                  wrapper_options=stats_args)

        tool_obj.launch()


def main():
    # Make Python Fire not use a pager when it prints a help text
    fire.core.Display = lambda lines, out: out.write('\n'.join(lines) + '\n')
    print(gen_app_banner('Development'))
    fire.Fire(DevCLI())


if __name__ == '__main__':
    main()
