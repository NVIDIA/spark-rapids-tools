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

from dataclasses import dataclass

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.pricing.price_provider import PriceProvider


@dataclass
class DatabricksCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        pass


@dataclass
class DatabricksPriceProvider(PriceProvider):
    """
    Provide costs of Databricks instances
    """
    name = 'Databricks'
    plan: str

    def _process_resource_configs(self):
        online_entries = self.pricing_configs[self.plan].get_value('catalog', 'onlineResources')
        if not self.cache_files:
            self.cache_files = {}
        if not self.resource_urls:
            self.resource_urls = {}
        for online_entry in online_entries:
            file_name = online_entry.get('localFile')
            file_key = file_name.split('-catalog')[0]
            self.cache_files[file_key] = FSUtil.build_path(self.cache_directory, file_name)
            # currently does not support online url
            # self.resource_urls[file_key] = online_entry.get('onlineURL')

    def _create_catalog(self):
        if not self.catalogs:
            self.catalogs = {}
        for file_key in self.cache_files:
            self.catalogs[file_key] = DatabricksCatalogContainer(prop_arg=self.cache_files[file_key])

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

    def get_instance_price(self, file_key: str, compute_type: str, instance_type: str, gpu_device: str) -> float:
        return self.catalogs[file_key].get_value(compute_type, instance_type, gpu_device)
