# Copyright (c) 2025, NVIDIA CORPORATION.
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

"""module that defines the logic of loading the reports."""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_tools import CspPath
from spark_rapids_tools.api_v1 import ToolResultHandlerT, ResultHandler, result_registry
from spark_rapids_tools.api_v1.report_reader import ToolReportReaderT, report_registry, PerAppToolReportReader, \
    ToolReportReader
from spark_rapids_tools.api_v1.table_definition import TableDef
from spark_rapids_tools.storagelib.cspfs import BoundedCspPath
from spark_rapids_tools.utils import AbstractPropContainer


@dataclass
class ReportStub(object):
    """
    A class the represents a dummy holder to load nested reports.
    After the first path, this meta-data is used to resolve all complex reports.
    """
    # unique identifier for the report
    report_id: str
    # the child report can be relatively located from the parent report
    relative_path: Optional[str] = field(default='')


class ReportDefinition(BaseModel):
    """
    Class used to load convert the properties into data objects.
    Note that the class uses pydantic to generate aliases for the fields. Otherwise, there will be
    an error reading from yaml/json files.
    """
    model_config = ConfigDict(alias_generator=to_camel)  # see config.py in Pydantic library
    # uuid for the report
    report_id: str
    # Description of the report. Mainly used for troubleshooting and documentation.
    description: str
    # the scope of the report. There are two types:
    # per-app: each table content is defined based on a single app
    # global: this is a summary report that is global.
    # In that category, output files are accessed directly without the need to add app-id prefix.
    scope: str
    # sub_directory is optional field. since a report can be nested or it can be a root, then we
    # need to define the sub_directory of the report where the files are located.
    # Otherwise, the callers need to initialize a report by passing the exact output folder instead
    # of the parent folder which defies the purpose of abstraction.
    sub_directory: Optional[str] = None
    # List of tables defined in that report
    table_definitions: List[TableDef] = Field(default_factory=list)
    # A report can be nested. For example, the wrapperReport has the coreOutput as a nested report.
    nested_reports: List[ReportStub] = Field(default_factory=list)

    def is_per_app(self) -> bool:
        return self.scope == 'per-app'

    def has_nested_reports(self) -> bool:
        if self.nested_reports:
            return True
        return False

    def get_table_defs(self) -> Dict[str, TableDef]:
        res: Dict[str, TableDef] = {}
        for t_def in self.table_definitions:
            res[t_def.label] = t_def
        return res


@dataclass
class ReportLoader(object):
    """
    A class that loads the report definitions from the provided configuration files.
    :param configs_path: a list of paths to the configuration files that define the reports.
    :param report_defs: a dictionary that holds the report definitions loaded from the configuration files.
                        The keys are the report IDs, and the values are the ReportDefinition objects.
    """
    configs_path: Optional[List[str]] = field(default_factory=list)
    report_defs: Dict[str, ReportDefinition] = field(default_factory=dict, init=False)

    def _load_from_configs(self, conf_path: str) -> None:
        """
        Load the report definitions from the provided configs.
        """
        try:
            yaml_container = AbstractPropContainer.load_from_file(file_path=conf_path)
            report_defs = yaml_container.get_value('reportDefinitions')
            for raw_def in report_defs:
                report_def = ReportDefinition(**raw_def)
                self.report_defs[report_def.report_id] = report_def
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError(f'Could not load report definitions from: {conf_path}') from e

    @property
    def core_report_dir(self) -> str:
        """
        Returns the core report directory.
        This is used to define the output configurations for the core reports.
        :return: the core report directory.
        """
        return Utils.resource_path('generated_files/core/reports')

    @property
    def wrap_report_dir(self) -> str:
        """
        Returns the core report directory.
        This is used to define the output directory for the core reports.
        :return: the core report directory.
        """
        return Utils.resource_path('reports')

    @property
    def core_report_definitions(self) -> List[str]:
        """
        Returns the list of core report definitions.
        This is used to define the output configurations for the core reports.
        :return: the list of core report definitions.
        """
        return [
            f'{self.core_report_dir}/qualCoreReport.yaml',
            f'{self.core_report_dir}/profCoreReport.yaml',
            f'{self.core_report_dir}/coreRawMetricsReport.yaml'
        ]

    @property
    def wrapper_report_definitions(self) -> List[str]:
        """
        Returns the list of core report definitions.
        This is used to define the output configurations for the core reports.
        :return: the list of core report definitions.
        """
        return [
            f'{self.wrap_report_dir}/profWrapperReport.yaml',
            f'{self.wrap_report_dir}/qualWrapperReport.yaml'
        ]

    def __post_init__(self):
        if not self.configs_path:
            # add the default paths for core report definitions and wrapper report definitions.
            self.configs_path.extend(self.wrapper_report_definitions)
            self.configs_path.extend(self.core_report_definitions)
        # iterate on all the definitions to load them.
        for config_path in self.configs_path:
            self._load_from_configs(config_path)

    def create_recursive_report_reader(self, report_id: str, out_path: BoundedCspPath,
                                       holder: Dict[str, ToolReportReaderT],
                                       nested_level: int = 0) -> None:
        """
        Recursively creates report reader instances for a given report ID and its nested reports.

        :param report_id: The unique identifier of the report to load.
        :param out_path: The output path (as BoundedCspPath) where report files are located.
        :param holder: A dictionary to collect created ToolReportReaderT instances, keyed by report
                        ID.
        :param nested_level: The current recursion depth, used to handle sub-directory logic.
        :return: None. The holder dictionary is updated in place.
        """
        rep_defn = self.report_defs.get(report_id)
        if rep_defn.sub_directory is not None and nested_level == 0:
            # check that the sub-directory is not added twice
            if out_path.base_name() != rep_defn.sub_directory:
                # add the sub-directory
                out_path = out_path.create_sub_path(rep_defn.sub_directory)
        if report_id in report_registry:
            target_class = report_registry.get(report_id)
        elif rep_defn.is_per_app():
            target_class = PerAppToolReportReader
        else:
            target_class = ToolReportReader
        for nest_stub in rep_defn.nested_reports:
            new_out_path = out_path.create_sub_path(nest_stub.relative_path)
            self.create_recursive_report_reader(nest_stub.report_id, new_out_path,
                                                holder,
                                                nested_level=nested_level+1)
        if rep_defn.table_definitions:
            report_reader = target_class(out_path=CspPath(out_path),
                                         report_id=report_id,
                                         table_definitions=rep_defn.get_table_defs())
            holder[report_id] = report_reader

    def create_result_handler(self,
                              report_id: str,
                              out_path: Union[str, BoundedCspPath]) -> ToolResultHandlerT:
        """
        Creates and returns a result handler for the specified report.

        :param report_id: The unique identifier of the report to handle.
        :param out_path: The output path (as a string or BoundedCspPath) where report files are located.
        :return: An instance of ToolResultHandlerT for the given report.
        """
        if isinstance(out_path, str):
            out_path = CspPath(out_path)
        readers: Dict[str, ToolReportReader] = {}
        self.create_recursive_report_reader(
            report_id=report_id, out_path=out_path, holder=readers)
        target_class = result_registry.get(report_id)
        if target_class is None:
            target_class = ResultHandler
        return target_class(report_id=report_id,
                            out_path=out_path,
                            readers=readers)
