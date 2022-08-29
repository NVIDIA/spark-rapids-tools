# Copyright (c) 2022, NVIDIA CORPORATION.
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

from dataclasses import dataclass, field

from spark_rapids_dataproc_tools.dataproc_utils import DataprocClusterPropContainer
from spark_rapids_dataproc_tools.utilities import AbstractPropertiesContainer, JSONPropertiesContainer


@dataclass
class PriceProvider(object):
    name: str
    catalog: AbstractPropertiesContainer
    meta: dict = field(default_factory=dict)

    def get_cpu_price(self) -> float:
        pass

    def get_ssd_price(self) -> float:
        pass

    def get_ram_price(self) -> float:
        pass

    def get_gpu_price(self) -> float:
        pass

    def setup(self, **kwargs) -> None:
        for key, value in kwargs.items():
            self.meta[key] = value


@dataclass
class DataprocCatalogContainer(JSONPropertiesContainer):
    def _init_fields(self) -> None:
        # the prices of the products are defined under 'gcp_price_list'
        self.props = self.props['gcp_price_list']


@dataclass
class DataprocPriceProvider(PriceProvider):
    def _get_machine_type(self) -> str:
        return self.meta.get('machineType')

    def _get_machine_prefix(self) -> str:
        return self._get_machine_type().split("-")[0]

    def __key_for_cpe_machine_cores(self) -> str:
        return 'CP-COMPUTEENGINE-{}-PREDEFINED-VM-CORE'.format(self._get_machine_prefix().upper())

    def __key_for_cpe_machine_ram(self) -> str:
        return 'CP-COMPUTEENGINE-{}-PREDEFINED-VM-RAM'.format(self._get_machine_prefix().upper())

    def __key_for_gpu_device(self) -> str:
        return 'GPU_NVIDIA_TESLA_{}'.format(self.meta.get('gpuType').upper())

    def __key_for_cpe_vm(self) -> str:
        return 'CP-COMPUTEENGINE-VMIMAGE-{}'.format(self._get_machine_type().upper())

    def __get_region(self) -> str:
        return self.meta.get('region')

    def get_cpu_price(self) -> float:
        lookup_key = self.__key_for_cpe_machine_cores()
        return self.catalog.get_value(lookup_key, self.__get_region())

    def get_ram_price(self) -> float:
        lookup_key = self.__key_for_cpe_machine_ram()
        return self.catalog.get_value(lookup_key, self.__get_region())

    def get_gpu_price(self) -> float:
        lookup_key = self.__key_for_gpu_device()
        return self.catalog.get_value(lookup_key, self.__get_region())

    def get_ssd_price(self) -> float:
        lookup_key = 'CP-COMPUTEENGINE-LOCAL-SSD'
        return self.catalog.get_value(lookup_key, self.__get_region()) * 0.513698630136986

    def get_dataproc_cluster_price(self) -> float:
        lookup_key = 'CP-DATAPROC'
        return self.catalog.get_value(lookup_key, 'us')

    def get_cores_count_for_vm(self) -> str:
        lookup_key = self.__key_for_cpe_vm()
        cores = self.catalog.get_value_silent(lookup_key, 'cores')
        return cores

    def get_ram_size_for_vm(self) -> str:
        lookup_key = self.__key_for_cpe_vm()
        memory = self.catalog.get_value_silent(lookup_key, 'memory')
        return memory


@dataclass
class DataprocSavingsEstimator(object):
    price_provider: DataprocPriceProvider
    gpu_device: str = 'T4'
    gpu_per_machine: int = 2
    comments = []
    cluster: DataprocClusterPropContainer = field(default=None, init=False)
    cost_with_gpu: float = field(default=None, init=False)
    cost_no_gpu: float = field(default=None, init=False)

    def __get_container_cost(self) -> float:
        return self.price_provider.get_dataproc_cluster_price()

    def calculate_master_cost(self) -> float:
        # setup the cost provider for the master node
        master_instances = self.cluster.get_master_vm_instances()
        master_region, master_zone, master_machine = self.cluster.get_master_machine_info()
        self.price_provider.setup(
            region=master_region,
            zone=master_zone,
            machineType=master_machine,
        )
        cores_count, ram_size = (self.price_provider.get_cores_count_for_vm(),
                                 self.price_provider.get_ram_size_for_vm())
        if cores_count is None or ram_size is None:
            # The machine type isn't found in the catalog therefore we will try to get the data from
            # gcloud describe command
            cores_count, ram_size = self.cluster.get_master_cpu_info()
            # TODO: memory here is in mb, we need to convert it to gb
            ram_size = float(ram_size) / 1024

        ssds_count = self.cluster.get_master_local_ssds()

        cores_cost = self.price_provider.get_cpu_price() * int(cores_count)
        memory_cost = self.price_provider.get_ram_price() * ram_size
        ssds_cost = self.price_provider.get_ssd_price() * ssds_count if (ssds_count > 0) else 0.0
        return master_instances * (cores_cost + memory_cost + ssds_cost)

    def calculate_workers_cost(self, include_gpu: bool = False) -> float:
        # setup the cost provider for the worker nodes
        worker_instances = self.cluster.get_worker_vm_instances()
        worker_region, worker_zone, worker_machine = self.cluster.get_worker_machine_info()
        if include_gpu:
            worker_machine, converted = self.cluster.convert_worker_machine_if_not_supported()
            if converted:
                self.comments.append(
                    f"To support acceleration with T4 GPUs, you will need to switch "
                    f"your worker node instance type to {worker_machine}")

        self.price_provider.setup(
            region=worker_region,
            zone=worker_zone,
            machineType=worker_machine,
            gpuType=self.gpu_device,
            gpuCount=self.gpu_per_machine
        )
        cores_count, ram_size = (self.price_provider.get_cores_count_for_vm(),
                                 self.price_provider.get_ram_size_for_vm())
        if cores_count is None or ram_size is None:
            # If the machine is converted, we may fallback here too.
            # The machine type isn't found in the catalog therefore we will try to get the data from
            # gcloud describe command
            cores_count, ram_size = self.cluster.get_cpu_info_for_machine_type(zone=worker_zone,
                                                                               machine_type=worker_machine)
            # memory here is in mb, we need to convert it to gb
            ram_size = float(ram_size) / 1024

        ssds_count = self.cluster.get_worker_local_ssds()
        cores_cost = self.price_provider.get_cpu_price() * int(cores_count)
        memory_cost = self.price_provider.get_ram_price() * ram_size
        gpu_cost = 0.0
        if include_gpu:
            gpu_unit_price = self.price_provider.get_gpu_price()
            gpu_cost = gpu_unit_price * self.gpu_per_machine
            # it is recommended to use local SSDs if GPU is enabled
            if ssds_count == 0:
                self.comments.append(
                    f"Worker nodes have no local SSDs. Local SSD is recommended for Spark scratch space to improve IO."
                    f"\n\tCost estimation will be based on 1 local SSd per worker.")
            ssds_count = max(1, ssds_count)
        ssds_cost = self.price_provider.get_ssd_price() * ssds_count if (ssds_count > 0) else 0.0
        return worker_instances * (cores_cost + memory_cost + ssds_cost + gpu_cost)

    def setup_calculations(self, cluster_inst: DataprocClusterPropContainer) -> None:
        self.cluster = cluster_inst
        container_cost = self.price_provider.get_dataproc_cluster_price()
        master_cost = self.calculate_master_cost()
        workers_cost = self.calculate_workers_cost()
        workers_cost_with_gpu = self.calculate_workers_cost(include_gpu=True)
        self.cost_no_gpu = container_cost + master_cost + workers_cost
        self.cost_with_gpu = container_cost + master_cost + workers_cost_with_gpu

    def get_costs_and_savings(self,
                              app_duration: float,
                              estimated_gpu_duration: float):
        estimated_cpu_cost = self.cost_no_gpu * app_duration / (60.0 * 60 * 1000)
        if estimated_cpu_cost <= 0.0:
            # avoid division by zero
            return 0.0, 0.0, 0.0
        estimated_gpu_cost = self.cost_with_gpu * estimated_gpu_duration / (60.0 * 60 * 1000)
        estimated_savings = 100.0 - ((100.0 * estimated_gpu_cost) / estimated_cpu_cost)
        return estimated_cpu_cost, estimated_gpu_cost, estimated_savings
