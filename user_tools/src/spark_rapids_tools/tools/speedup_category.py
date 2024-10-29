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

"""Implementation class for Speedup Category logic."""

from dataclasses import dataclass, field
from typing import Optional, Dict

import pandas as pd

from spark_rapids_pytools.cloud_api.sp_types import PlatformBase
from spark_rapids_tools import CspEnv
from spark_rapids_tools.enums import AppExecutionType
from spark_rapids_tools.utils import Utilities


@dataclass
class SpeedupStrategy:
    """
    Wrapper class for speedup strategy properties.
    """
    _categories: list = field(default_factory=list, init=False)
    _eligibility_conditions: list = field(default_factory=list, init=False)

    def __init__(self, props: dict):
        self._categories = props.get('categories', [])
        self._eligibility_conditions = props.get('eligibilityConditions', [])

    def get_categories(self) -> list:
        return self._categories

    def get_eligibility_conditions(self) -> list:
        return self._eligibility_conditions


@dataclass
class SpeedupStrategyBuilder:
    """"
    Builder class for creating speedup strategy based on Spark properties.
    TODO: This class can be extended to support different speedup strategies for a mixed set of application types.
    """

    @classmethod
    def build_speedup_strategies(cls,
                                 platform: PlatformBase,
                                 spark_properties: dict,
                                 speedup_strategy_props: dict) -> Dict[str, SpeedupStrategy]:
        """
        Builds a SpeedupStrategy based on the provided Spark properties of the applications.
        This function verifies that all applications belong to the same type and returns the appropriate strategy.

        :param platform: Platform for which the speedup strategy is being built.
        :param spark_properties: Dictionary of App IDs and corresponding Spark properties.
        :param speedup_strategy_props: Dictionary containing the properties for different speedup strategies.
        """
        default_strategy = SpeedupStrategy(speedup_strategy_props.get(AppExecutionType.get_default()))

        # For non-Databricks platforms, return the default speedup strategy (i.e. Spark CPU based)
        if platform.get_platform_name() not in [CspEnv.DATABRICKS_AWS, CspEnv.DATABRICKS_AZURE]:
            return {app_id: default_strategy for app_id in spark_properties.keys()}

        speedup_strategies = {}
        spark_version_key = 'spark.databricks.clusterUsageTags.sparkVersion'

        # Detect the application type based on the Spark version
        for app_id, spark_properties_df in spark_properties.items():
            spark_props_dict = Utilities.convert_df_to_dict(spark_properties_df)
            spark_version = spark_props_dict.get(spark_version_key, '').lower()
            if AppExecutionType.PHOTON in spark_version:
                speedup_strategies[app_id] = SpeedupStrategy(speedup_strategy_props.get(AppExecutionType.PHOTON))
            else:
                speedup_strategies[app_id] = default_strategy

        return speedup_strategies


@dataclass
class SpeedupCategory:
    """
    Encapsulates the logic to categorize the speedup values based on the range values.
    """
    props: dict = field(default=None, init=True)

    def __build_category_column(self,
                                all_apps: pd.DataFrame,
                                speedup_strategies: Dict[str, SpeedupStrategy]) -> pd.DataFrame:
        """
        Build the category column based on the range values of the speedup column.
        Example:
        props['categories'] = [
            {'title': 'Not Recommended', 'lowerBound': -100000, 'upperBound': 1.3},
            {'title': 'Small',           'lowerBound': 1.3,     'upperBound': 2},
            {'title': 'Medium',          'lowerBound': 2,       'upperBound': 3},
            {'title': 'Large',           'lowerBound': 3,       'upperBound': 100000}
        ]
        1. input: row_1 = pd.Series({'speedup': 1.8})
           output: row_1 = pd.Series({'speedup': 1.8, 'speedup category': 'Small'})
           reason: Speedup Category will be 'Small' because the speedup is within the range (1.3-2).
        2. input: row_2 = pd.Series({'speedup': 3.5})
           output: row_2 = pd.Series({'speedup': 3.5, 'speedup category': 'Large'})
           reason: Speedup Category will be 'Large' because the speedup is within the range (3-100000).
        """
        category_col_name = self.props.get('categoryColumnName')
        speedup_col_name = self.props.get('speedupColumnName')

        # Calculate the category based on the speedup value
        def calculate_category(single_row: pd.Series) -> Optional[str]:
            app_id = single_row.get('App ID')
            categories = speedup_strategies[app_id].get_categories()
            col_value = single_row.get(speedup_col_name)
            for category in categories:
                if category.get('lowerBound') <= col_value < category.get('upperBound'):
                    return category.get('title')
            return None
        all_apps[category_col_name] = all_apps.apply(calculate_category, axis=1)
        return all_apps

    def __process_category(self,
                           all_apps: pd.DataFrame,
                           speedup_strategies: Dict[str, SpeedupStrategy]) -> pd.DataFrame:
        """
        Process the speedup category column based on the eligibility criteria. If the row does not match
        the criteria, the category column will be set to the `Not Recommended` category.
        Example:
        self.props['eligibilityConditions'] = [
            {'columnName': 'criteriaCol1', 'lowerBound': 18, 'upperBound': 30},
            {'columnName': 'criteriaCol2', 'lowerBound': 70, 'upperBound': 100}
        ]
        1. input: row_1 = pd.Series({'criteriaCol1': 25, 'criteriaCol2': 85, 'speedup category': 'Large'})
           output: row_1 = pd.Series({'criteriaCol1': 25, 'criteriaCol2': 85, 'speedup category': 'Large'})
           reason: Category will remain 'Large' because the criteriaCol1 is within the range (18-30) and
            the criteriaCol2 (85) is within the range (70-100).
        2. input: row_2 = pd.Series({'criteriaCol1': 15, 'criteriaCol2': 85, 'speedup category': 'Medium'})
           output: row_2 = pd.Series({'criteriaCol1': 15, 'criteriaCol2': 85, 'speedup category': 'Not Recommended'})
           reason: Category will be set to 'Not Recommended' because the criteriaCol1 is not within the range (18-30)
        """
        category_col_name = self.props.get('categoryColumnName')
        heuristics_col_name = self.props.get('heuristicsColumnName')

        def process_row(single_row: pd.Series) -> str:
            app_id = single_row.get('App ID')
            eligibility_conditions = speedup_strategies[app_id].get_eligibility_conditions()
            for entry in eligibility_conditions:
                col_value = single_row[entry.get('columnName')]
                # If the row is marked to be skipped by heuristics or the value is not within the range,
                # set the category to default category (Not Recommended)
                if (single_row.get(heuristics_col_name) is True or
                        not entry.get('lowerBound') <= col_value <= entry.get('upperBound')):
                    return self.props.get('defaultCategory')
            return single_row.get(category_col_name)

        all_apps[category_col_name] = all_apps.apply(process_row, axis=1)
        return all_apps

    def build_category_column(self,
                              all_apps: pd.DataFrame,
                              speedup_strategies: Dict[str, SpeedupStrategy]) -> pd.DataFrame:
        apps_with_category = self.__build_category_column(all_apps, speedup_strategies)
        processed_apps = self.__process_category(apps_with_category, speedup_strategies)
        return processed_apps
