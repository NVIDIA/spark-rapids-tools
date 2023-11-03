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

"""providing absolute costs of resources in GCloud DataprocGke"""

from dataclasses import dataclass

from spark_rapids_pytools.pricing.dataproc_pricing import DataprocPriceProvider


@dataclass
class DataprocGkePriceProvider(DataprocPriceProvider):
    """
    Provide costs of DataprocGke instances
    """
    name = 'DataprocGke'

    def get_container_cost(self) -> float:
        gke_container_cost = self.__get_gke_container_cost()
        return gke_container_cost

    def __get_gke_container_cost(self) -> float:
        lookup_key = 'CP-GKE-CONTAINER-MANAGMENT-COST'
        return self.catalogs['gcloud'].get_value(lookup_key, 'us')
