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

"""providing absolute costs of resources in GCloud Dataproc"""

from dataclasses import dataclass


from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.pricing.price_provider import PriceProvider


@dataclass
class DataprocCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        # the prices of the products are defined under 'gcp_price_list'
        self.props = self.props['gcp_price_list']


@dataclass
class DataprocPriceProvider(PriceProvider):
    """
    Provide costs of Dataproc instances
    """
    name = 'Dataproc'

    def _process_resource_configs(self):
        online_entries = self.pricing_configs['gcloud'].get_value('catalog', 'onlineResources')
        for online_entry in online_entries:
            if online_entry.get('resourceKey') == 'gcloud-catalog':
                file_name = online_entry.get('localFile')
                self.cache_files = {'gcloud': FSUtil.build_path(self.cache_directory, file_name)}
                self.resource_urls = {'gcloud': online_entry.get('onlineURL')}
                break

    def _create_catalogs(self):
        self.catalogs = {'gcloud': DataprocCatalogContainer(prop_arg=self.cache_files['gcloud'])}

    def get_ssd_price(self, machine_type: str) -> float:
        lookup_key = 'CP-COMPUTEENGINE-LOCAL-SSD'
        ssd_unit_size_factor = float(self.pricing_configs['gcloud'].get_value('catalog', 'ssd', 'unitSizeFactor'))
        return self.catalogs['gcloud'].get_value(lookup_key, self.region) * ssd_unit_size_factor

    def get_ram_price(self, machine_type: str) -> float:
        lookup_key = self._key_for_cpe_machine_ram(machine_type)
        return self.catalogs['gcloud'].get_value(lookup_key, self.region)

    def get_gpu_price(self, gpu_device: str) -> float:
        lookup_key = self._key_for_gpu_device(gpu_device)
        return self.catalogs['gcloud'].get_value(lookup_key, self.region)

    def get_cpu_price(self, machine_type: str) -> float:
        lookup_key = self._key_for_cpe_machine_cores(machine_type)
        return self.catalogs['gcloud'].get_value(lookup_key, self.region)

    def get_container_cost(self) -> float:
        return self.__get_dataproc_cluster_price()

    def __get_dataproc_cluster_price(self) -> float:
        lookup_key = 'CP-DATAPROC'
        return self.catalogs['gcloud'].get_value(lookup_key, 'us')

    def get_cores_count_for_vm(self, machine_type: str) -> str:
        lookup_key = self._key_for_cpe_vm(machine_type)
        cores = self.catalogs['gcloud'].get_value_silent(lookup_key, 'cores')
        return cores

    def get_ram_size_for_vm(self, machine_type: str) -> str:
        lookup_key = self._key_for_cpe_vm(machine_type)
        memory = self.catalogs['gcloud'].get_value_silent(lookup_key, 'memory')
        return memory

    @classmethod
    def _key_for_cpe_machine_cores(cls, machine_type: str) -> str:
        return f'CP-COMPUTEENGINE-{cls._get_machine_prefix(machine_type).upper()}-PREDEFINED-VM-CORE'

    @classmethod
    def _key_for_cpe_machine_ram(cls, machine_type: str) -> str:
        return f'CP-COMPUTEENGINE-{cls._get_machine_prefix(machine_type).upper()}-PREDEFINED-VM-RAM'

    @classmethod
    def _key_for_gpu_device(cls, gpu_device: str) -> str:
        return f'GPU_NVIDIA_TESLA_{gpu_device.upper()}'

    @classmethod
    def _get_machine_prefix(cls, machine_type: str) -> str:
        return machine_type.split('-')[0]

    @classmethod
    def _key_for_cpe_vm(cls, machine_type: str):
        return f'CP-COMPUTEENGINE-VMIMAGE-{machine_type.upper()}'
