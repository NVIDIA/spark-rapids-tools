# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Module to combine outputs from multiple executors. """

import os
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from logging import Logger

from spark_rapids_tools_distributed.output_processing.processors import CSVOutputProcessor, JSONOutputProcessor, \
    LogOutputProcessor, CopyOutputProcessor
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools.storagelib import HdfsPath, CspFs, LocalPath


@dataclass
class OutputCombiner:
    """ Class to combine outputs from multiple executors. """

    jar_output_folder: LocalPath = field(init=True)
    executor_output_dir: HdfsPath = field(init=True)
    logger: Logger = field(default=None, init=False)

    def __post_init__(self):
        self.logger = ToolLogging.get_and_setup_logger('rapids.tools.distributed.output_combiner')

    @abstractmethod
    def combine(self):
        """
        Abstract method to combine all outputs. This method should be implemented by the specific tool output combiner.
        """
        raise NotImplementedError


@dataclass
class QualificationOutputCombiner(OutputCombiner):
    """
    This class implements the combine_outputs method for combining outputs from multiple executors
    for the Qualification tool.

    Structure of the output folder:
    ├── rapids_4_spark_qualification_output.csv
    ├── rapids_4_spark_qualification_output_cluster_information.json
    ├── rapids_4_spark_qualification_output.log
    ├── rapids_4_spark_qualification_stderr.log
    ***
    ├── raw_metrics/
    ├── tuning/
    └── runtime.properties
    """

    def combine(self):
        """Main method to combine all outputs."""
        self.logger.info('Combining outputs from %s to %s', self.executor_output_dir, self.jar_output_folder)
        # Process the tools output directories
        # e.g.
        # task_output_dirs:   [hdfs:///path/qual_2024xxx/<eventlog_name>, ...]
        # tools_output_dirs:  [hdfs:///path/qual_2024xxx/<eventlog_name>/rapids_4_spark_qualification_output, ...]
        task_output_dirs = CspFs.list_all_dirs(self.executor_output_dir)
        tools_output_dirs = [
            tools[0] if tools else None
            for app_output_dir in task_output_dirs
            for tools in [CspFs.list_all_dirs(app_output_dir)]
        ]

        # Initialize the processors
        csv_processor = CSVOutputProcessor(self.jar_output_folder)
        json_processor = JSONOutputProcessor(self.jar_output_folder)
        log_processor = LogOutputProcessor(self.jar_output_folder)
        copy_processor = CopyOutputProcessor(self.jar_output_folder)

        runtime_properties_file = self.jar_output_folder.create_sub_path('runtime.properties')

        def process_directory(tools_output_dir):
            if tools_output_dir:
                # Use the specific processors for different file types
                csv_processor.process(tools_output_dir)
                json_processor.process(tools_output_dir)
                log_processor.process(tools_output_dir)
                copy_processor.process(tools_output_dir, resource_name='raw_metrics')
                copy_processor.process(tools_output_dir, resource_name='tuning')
                # Runtime properties file is only copied once
                if not runtime_properties_file.exists():
                    copy_processor.process(tools_output_dir, resource_name='runtime.properties')

        # Set max_workers to the minimum of the number of directories or (CPU count + 4).
        # Unlike the default ThreadPoolExecutor behavior, we do not cap max_workers at 32.
        max_workers = min(len(tools_output_dirs), (os.cpu_count() or 1) + 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(process_directory, tools_output_dirs)

        csv_processor.write_data()
        json_processor.write_data()
        log_processor.write_data()
