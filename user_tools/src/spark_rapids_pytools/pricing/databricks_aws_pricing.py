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

"""providing absolute costs of resources in Databricks"""

from dataclasses import dataclass, field
from typing import Optional

from spark_rapids_tools import get_elem_from_dict, get_elem_non_safe
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.pricing.emr_pricing import AWSCatalogContainer
from spark_rapids_pytools.pricing.price_provider import PriceProvider


@dataclass
class DBAWSCatalogContainer():
    """
    Db-AWS pricing catalog. It is initialized by a DB-AWS catalog file.
    The final pricing will be loaded inside a dictionary for lookup.
    Currently only processes 'Premium' plan and 'Jobs Compute' type.
    """
    catalog_file: str
    props: dict = field(default_factory=dict, init=False)  # instance -> price [str, float]

    def get_value(self, *key_strs) -> Optional[float]:
        return get_elem_from_dict(self.props, key_strs)

    def get_value_silent(self, *key_strs) -> Optional[float]:
        return get_elem_non_safe(self.props, key_strs)

    def __post_init__(self) -> None:
        self.props = {}
        raw_props = JSONPropertiesContainer(self.catalog_file)
        for elem in raw_props.props:
            if elem.get('plan') == 'Premium' and elem.get('compute') == 'Jobs Compute':
                self.props[elem.get('instance')] = float(elem.get('hourrate'))


@dataclass
class DatabricksAWSPriceProvider(PriceProvider):
    """
    Provide costs of Databricks instances
    """
    name = 'Databricks-AWS'
    # TODO: current default to 'Premium' plan and 'Jobs Compute' compute type,
    # need to figure out how to find these values from cluster properties.
    plan: str = field(default='Premium', init=False)  # Standard, Premium (default), or Enterprise

    def _process_resource_configs(self) -> None:
        online_entries = self.pricing_configs['databricks-aws'].get_value('catalog', 'onlineResources')
        for online_entry in online_entries:
            file_name = online_entry.get('localFile')
            file_key = online_entry.get('resourceKey').split('-catalog')[0]
            self.cache_files[file_key] = FSUtil.build_path(self.cache_directory, file_name)
            self.resource_urls[file_key] = online_entry.get('onlineURL')

    def _create_catalogs(self) -> None:
        ec2_cached_files = {'ec2': self.cache_files['ec2']}
        self.catalogs = {'aws': AWSCatalogContainer(ec2_cached_files)}
        self.catalogs['databricks-aws'] = DBAWSCatalogContainer(self.cache_files['databricks-aws'])
