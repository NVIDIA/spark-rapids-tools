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
class DatabricksStandardCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        pass


@dataclass
class DatabricksPremiumCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        pass


@dataclass
class DatabricksEnterpriseCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        pass


@dataclass
class DatabricksPriceProvider(PriceProvider):
    """
    Provide costs of Databricks instances
    """
    name = 'Databricks'

    def _process_resource_configs(self):
        pass

        # online_entries = self.pricing_config.get_value('catalog', 'onlineResources')
        # for online_entry in online_entries:
        #     if online_entry.get('resourceKey') == 'gcloud-catalog':
        #         file_name = online_entry.get('localFile')
        #         self.cache_file = FSUtil.build_path(self.cache_directory, file_name)
        #         self.resource_url = online_entry.get('onlineURL')
        #         break

    def _create_catalog(self):
        pass

    def get_ssd_price(self, machine_type: str) -> float:
        pass

    def get_ram_price(self, machine_type: str) -> float:
        pass

    def get_gpu_price(self, plan: str, compute_type: str, gpu_device: str) -> float:
        catalog_file = self.get_cache_file(plan)
        return self.get_catalog(plan).get_value(compute_type, gpu_device)

    def get_cpu_price(self, machine_type: str) -> float:
        pass

    def get_container_cost(self) -> float:
        pass

    def __get_dataproc_cluster_price(self) -> float:
        pass

    def get_cores_count_for_vm(self, machine_type: str) -> str:
        pass

    def get_ram_size_for_vm(self, machine_type: str) -> str:
        pass

    @classmethod
    def _key_for_cpe_machine_cores(cls, machine_type: str) -> str:
        pass

    @classmethod
    def _key_for_cpe_machine_ram(cls, machine_type: str) -> str:
       pass

    @classmethod
    def _key_for_gpu_device(cls, gpu_device: str) -> str:
        pass

    @classmethod
    def _get_machine_prefix(cls, machine_type: str) -> str:
        pass

    @classmethod
    def _key_for_cpe_vm(cls, machine_type: str):
        pass
