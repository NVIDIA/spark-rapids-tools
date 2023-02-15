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

"""providing absolute costs of resources in AWS"""

import datetime
import os
from dataclasses import dataclass, field

import requests

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.pricing.price_provider import PriceProvider


@dataclass
class EmrEc2CatalogContainer(JSONPropertiesContainer):
    """
    https://aws.amazon.com/emr/pricing/#Pricing_Examples
    The Amazon EMR price is added to the Amazon EC2 price (the price for the underlying servers).
    """
    ec2_prices_path: str = None

    def _process_prices(self):
        sku_to_instance_type = {}
        for sku in self.get_value('products'):
            if sw_type := self.get_value_silent('products', sku, 'attributes', 'softwareType'):
                if sw_type == 'EMR':
                    sku_to_instance_type[sku] = self.get_value('products', sku, 'attributes', 'instanceType')
        emr_prices = {}
        for sku, instance_type in sku_to_instance_type.items():
            sku_info = self.get_value('terms', 'OnDemand', sku)
            _, sku_info_value = sku_info.popitem()
            price_dimensions = sku_info_value['priceDimensions']
            _, price_dimensions_value = price_dimensions.popitem()
            price = float(price_dimensions_value['pricePerUnit']['USD'])
            emr_prices[instance_type] = price

        ec2_props = JSONPropertiesContainer(self.ec2_prices_path)
        ec2_sku_to_instance_type = {}
        cond_dict = {
            'tenancy': 'Shared',
            'operatingSystem': 'Linux',
            'operation': 'RunInstances',
            'capacitystatus': 'Used'
        }
        for sku in ec2_props.get_value('products'):
            if attr := ec2_props.get_value_silent('products', sku, 'attributes'):
                precheck = True
                for cond_k, cond_v in cond_dict.items():
                    precheck = precheck and attr.get(cond_k) == cond_v
                if precheck:
                    ec2_sku_to_instance_type[sku] = attr['instanceType']
        ec2_prices = {}
        for sku in ec2_sku_to_instance_type:
            instance_type = ec2_sku_to_instance_type.get(sku)
            sku_info = ec2_props.get_value('terms', 'OnDemand', sku)
            _, sku_info_value = sku_info.popitem()
            price_dimensions = sku_info_value['priceDimensions']
            _, price_dimensions_value = price_dimensions.popitem()
            price = float(price_dimensions_value['pricePerUnit']['USD'])

            ec2_prices[instance_type] = price
        self.props = {'ec2': ec2_prices, 'emr': emr_prices}

    def _init_fields(self):
        self._process_prices()

    def get_emr_price(self, instance_type):
        return self.get_value('emr', instance_type)

    def get_ec2_price(self, instance_type):
        return self.get_value('ec2', instance_type)


@dataclass
class EMREc2PriceProvider(PriceProvider):
    """
    Provide costs of EMR running on Ec2 instances
    """
    name = 'Emr-Ec2'
    ec2_catalog_path: str = field(default=None, init=False)
    ec2_prices_url: str = field(default=None, init=False)

    @classmethod
    def get_cache_file_path(cls, comp,  region) -> str:
        file_name = f'emr_ec2_catalog_{comp}_{region}.json'
        # get folder from cache folder
        folder_path = Utils.get_rapids_tools_env('CACHE_FOLDER')
        return FSUtil.build_path(folder_path, file_name)

    def __regenerate_catalog_resource(self):
        # TODO: load constant values from configurations files
        aws_url_base = 'https://pricing.us-east-1.amazonaws.com'
        emr_region_ind_url = f'{aws_url_base}/offers/v1.0/aws/ElasticMapReduce/current/region_index.json'
        emr_region_resp = requests.get(emr_region_ind_url, timeout=60)
        emr_region_relative_url = emr_region_resp.json()['regions'][self.region]['currentVersionUrl']
        ec2_region_ind_url = f'{aws_url_base}/offers/v1.0/aws/AmazonEC2/current/region_index.json'
        ec2_region_resp = requests.get(ec2_region_ind_url, timeout=60)
        ec2_region_relative_url = ec2_region_resp.json()['regions'][self.region]['currentVersionUrl']

        self.resource_url = f'{aws_url_base}{emr_region_relative_url}'
        self.ec2_prices_url = f'{aws_url_base}{ec2_region_relative_url}'
        self._init_cache_file()

    def _caches_expired(self) -> bool:
        for c_file in [self.cache_file, self.ec2_catalog_path]:
            if not os.path.exists(c_file):
                return True
            modified_time = os.path.getmtime(c_file)
            diff_time = int(datetime.datetime.now().timestamp() - modified_time)
            if diff_time > self.cache_expiration_secs:
                return True
        return False

    def _init_cache_file(self):
        super()._init_cache_file()
        file_updated = FSUtil.cache_from_url(self.ec2_prices_url, self.ec2_catalog_path)
        self.logger.info('The catalog file for EC2 %s is %s',
                         self.cache_file,
                         'updated' if file_updated else 'is not modified, using the cached content')

    def _init_catalog(self):
        self.cache_file = self.get_cache_file_path('emr', self.region)
        self.ec2_catalog_path = self.get_cache_file_path('ec2', self.region)
        if self._caches_expired():
            self.__regenerate_catalog_resource()
        self._create_catalog()

    def _create_catalog(self):
        self.catalog = EmrEc2CatalogContainer(self.cache_file, ec2_prices_path=self.ec2_catalog_path)
