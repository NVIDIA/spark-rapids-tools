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

"""Cost estimator implementation based on a simplified model."""

from dataclasses import dataclass, field

from spark_rapids_dataproc_tools.dataproc_utils import DataprocClusterPropContainer, get_incompatible_criteria
from spark_rapids_dataproc_tools.utilities import AbstractPropertiesContainer, JSONPropertiesContainer


@dataclass
class PriceProvider(object):
    """
    An abstract class that represents interface to retirive costs of hardware configurations.
    """
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
    """
    Implementation of cost provider for Dataproc.
    """
    def _get_machine_type(self) -> str:
        return self.meta.get('machineType')

    def _get_machine_prefix(self) -> str:
        return self._get_machine_type().split('-')[0]

    def __key_for_cpe_machine_cores(self) -> str:
        return f'CP-COMPUTEENGINE-{self._get_machine_prefix().upper()}-PREDEFINED-VM-CORE'

    def __key_for_cpe_machine_ram(self) -> str:
        return f'CP-COMPUTEENGINE-{self._get_machine_prefix().upper()}-PREDEFINED-VM-RAM'

    def __key_for_gpu_device(self) -> str:
        return f'GPU_NVIDIA_TESLA_{self.meta.get("gpuType").upper()}'

    def __key_for_cpe_vm(self) -> str:
        return f'CP-COMPUTEENGINE-VMIMAGE-{self._get_machine_type().upper()}'

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
    """
    Implementation of model to get an estimate of cost savings.
    """
    price_provider: DataprocPriceProvider
    gpu_device: str = 'T4'
    gpu_per_machine: int = 2
    comments = []
    target_cluster: DataprocClusterPropContainer = field(default=None, init=False)
    source_cluster: DataprocClusterPropContainer = field(default=None, init=False)
    cost_with_gpu: float = field(default=None, init=False)
    cost_no_gpu: float = field(default=None, init=False)

    def calculate_master_cost(self, cluster_inst: DataprocClusterPropContainer) -> float:
        # setup the cost provider for the master node
        master_instances = cluster_inst.get_master_vm_instances()
        master_region, master_zone, master_machine = cluster_inst.get_master_machine_info()
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
            cores_count, ram_size = cluster_inst.get_master_cpu_info()
            # memory here is in mb, we need to convert it to gb
            ram_size = float(ram_size) / 1024

        ssds_count = cluster_inst.get_master_local_ssds()

        cores_cost = self.price_provider.get_cpu_price() * int(cores_count)
        memory_cost = self.price_provider.get_ram_price() * ram_size
        ssds_cost = self.price_provider.get_ssd_price() * ssds_count if (ssds_count > 0) else 0.0
        return master_instances * (cores_cost + memory_cost + ssds_cost)

    def calculate_workers_cost(self,
                               cluster_inst: DataprocClusterPropContainer,
                               include_gpu: bool = False) -> float:
        # setup the cost provider for the worker nodes
        worker_instances = cluster_inst.get_worker_vm_instances()
        worker_region, worker_zone, worker_machine = cluster_inst.get_worker_machine_info()
        if include_gpu:
            incompatible_type = cluster_inst.convert_worker_machine_if_not_supported()
            if len(incompatible_type) > 0:
                # machine has been converted
                worker_machine = incompatible_type.get('machineType')
                self.comments.append(incompatible_type.get('comments')['machineType'])
            incompatible_version = get_incompatible_criteria(imageVersion=cluster_inst.get_image_version())
            if len(incompatible_version) > 0:
                self.comments.append(incompatible_version.get('comments')['imageVersion'])

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
            cores_count, ram_size = cluster_inst.get_cpu_info_for_machine_type(zone=worker_zone,
                                                                               machine_type=worker_machine)
            # memory here is in mb, we need to convert it to gb
            ram_size = float(ram_size) / 1024

        ssds_count = cluster_inst.get_worker_local_ssds()
        cores_cost = self.price_provider.get_cpu_price() * int(cores_count)
        memory_cost = self.price_provider.get_ram_price() * ram_size
        gpu_cost = 0.0
        if include_gpu:
            gpu_unit_price = self.price_provider.get_gpu_price()
            gpu_cost = gpu_unit_price * self.gpu_per_machine
            # it is recommended to use local SSDs if GPU is enabled
            incompatible_ssd = get_incompatible_criteria(workerLocalSSDs=ssds_count)
            if len(incompatible_ssd) > 0:
                ssd_comments = [incompatible_ssd.get('comments')['workerLocalSSDs'],
                                'Cost estimation is based on 1 local SSD per worker.']
                self.comments.extend(ssd_comments)
            ssds_count = max(1, ssds_count)
        ssds_cost = self.price_provider.get_ssd_price() * ssds_count if (ssds_count > 0) else 0.0
        return worker_instances * (cores_cost + memory_cost + ssds_cost + gpu_cost)

    def setup_calculations(self,
                           original_cluster_inst: DataprocClusterPropContainer,
                           migration_cluster_inst: DataprocClusterPropContainer) -> None:
        self.target_cluster = migration_cluster_inst
        self.source_cluster = original_cluster_inst
        container_cost = self.price_provider.get_dataproc_cluster_price()
        source_master_cost = self.calculate_master_cost(self.source_cluster)
        source_workers_cost = self.calculate_workers_cost(self.source_cluster)
        target_master_cost = self.calculate_master_cost(self.target_cluster)
        target_workers_cost = self.calculate_workers_cost(self.target_cluster, include_gpu=True)

        self.cost_no_gpu = container_cost + source_master_cost + source_workers_cost
        self.cost_with_gpu = container_cost + target_master_cost + target_workers_cost

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
