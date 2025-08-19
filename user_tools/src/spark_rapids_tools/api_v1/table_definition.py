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

"""module that contains the definitions of tables for the tools wrapper"""
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from spark_rapids_tools.api_v1 import APIUtils
from spark_rapids_tools.enums import ReportTableFormat


@dataclass
class TableColumnDef(object):
    """Represents a column definition in a table."""
    name: str
    data_type: str
    description: str


class TableDef(BaseModel):
    """
    Represents a table definition for reporting.
    This class extends Pydantic BaseModel in order to use aliad generator to convert
    yaml loaded keys into acceptable object field names
    """
    model_config = ConfigDict(alias_generator=to_camel)  # see config.py in Pydantic library
    label: str
    description: str
    file_name: str
    scope: str
    columns: Optional[List[TableColumnDef]] = Field(default_factory=list)
    file_format: ReportTableFormat = ReportTableFormat.get_default()

    def get_column_by_name(self, name: str) -> Optional[TableColumnDef]:
        """Get a column definition by name."""
        for column in self.columns:
            if column.name == name:
                return column
        return None

    def get_column_names(self) -> List[str]:
        """Get all column names."""
        return [column.name for column in self.columns]

    def convert_to_schema(self) -> Dict[str, str]:
        """
        Creates a dictionary that represents the schema of a table/compatible
        with Pandas
        :return: dictionary compatible with pandas dataframe.
        """
        res = {}
        for col in self.columns:
            res[col.name] = APIUtils.scala_to_pd_dtype(col.data_type)
        return res

    def create_empty_df(self) -> pd.DataFrame:
        """
        converts the given table definition into a Pandas Dataframe.
        This includes the column names and the column data types.
        :return:
        """
        schema = self.convert_to_schema()
        return pd.DataFrame({
            col: pd.Series(dtype=dtype)
            for col, dtype in schema.items()
        })

    def to_json(self) -> Dict[str, Any]:
        return self.model_dump(mode='json')

    def accepts(self, f_format: Union[str, ReportTableFormat]) -> bool:
        """
        Check if the given file format is acceptable to parse the generated table.
        for example, we cannot read a java properties formatted file using a CSV parser.
        However, we can read a CSV formatted file using a Text parser
        :param f_format: The file format to check.
        :return: True if the file format is valid, False otherwise.
        """
        return ReportTableFormat(self.file_format).compatible(
            ReportTableFormat(f_format) if isinstance(f_format, str) else f_format
        )
