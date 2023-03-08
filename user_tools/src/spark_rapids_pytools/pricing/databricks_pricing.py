# Copyright (c) 2023, NVIDIA CORPORATION.
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

import requests

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.pricing.price_provider import PriceProvider


@dataclass
class DatabricksCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        pass


@dataclass
class DatabricksPriceProvider(EMREc2PriceProvider):
    """
    Provide costs of Databricks instances
    """
    name = 'Databricks'
    plan: str = field(default='databricks-premium', init=False)  # standard, premium (default), or enterprise
    ec2_catalog_path: str = field(default=None, init=False)
    ec2_prices_url: str = field(default=None, init=False)

    def _generate_cache_files(self):
        FSUtil.copy_resource(f'../resources/{self.plan}-catalog.json', self.cache_files[self.plan])
        super()._generate_cache_files()

    def _process_resource_configs(self):
        super._process_resource_configs()
        online_entries = self.pricing_configs[self.plan].get_value('catalog', 'onlineResources')
        for online_entry in online_entries:
            file_name = online_entry.get('localFile')
            file_key = file_name.split('-catalog')[0]
            self.cache_files[file_key] = FSUtil.build_path(self.cache_directory, file_name)

    def _create_catalog(self):
        super._create_catalog()
        for file_key, cache_file in self.cache_files.items():
            self.catalogs[file_key] = DatabricksCatalogContainer(prop_arg=cache_file)

    def get_ssd_price(self, machine_type: str) -> float:
        pass

    def get_ram_price(self, machine_type: str) -> float:
        pass

    def get_gpu_price(self, gpu_device: str) -> float:
        pass

    def get_cpu_price(self, machine_type: str) -> float:
        pass

    def get_container_cost(self) -> float:
        pass

    def get_cores_count_for_vm(self, machine_type: str) -> str:
        pass

    def get_ram_size_for_vm(self, machine_type: str) -> str:
        pass

    def get_instance_price(self, compute_type: str ='Jobs Compute', gpu_device: str) -> float:
        return self.catalogs[self.plan].get_value(compute_type, gpu_device, 'Rate($/hour)')
