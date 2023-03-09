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

from dataclasses import dataclass, field

import requests

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer, get_elem_from_dict, get_elem_non_safe
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.pricing.price_provider import PriceProvider


@dataclass
class AWSCatalogContainer:
    """
    An AWS pricing catalog. It is initialized by a list of catalog_files.
    The final pricing will be loaded inside a dictionary for lookup
    """
    catalog_files: dict  # [str, str]
    props: dict = field(default_factory=dict, init=False)

    def __load_instance_type_price_by_sku(self,
                                          comp_key: str,
                                          comp_props: JSONPropertiesContainer,
                                          sku_to_instance_type: dict):
        price_map = {}
        for sku, instance_type in sku_to_instance_type.items():
            sku_info = comp_props.get_value('terms', 'OnDemand', sku)
            _, sku_info_value = sku_info.popitem()
            price_dimensions = sku_info_value['priceDimensions']
            _, price_dimensions_value = price_dimensions.popitem()
            price = float(price_dimensions_value['pricePerUnit']['USD'])
            price_map[instance_type] = price
        self.props.update({comp_key: price_map})

    def _load_instance_types_emr(self, prop_key: str, catalog_file: str):
        emr_props = JSONPropertiesContainer(catalog_file)
        sku_to_instance_type = {}
        for sku in emr_props.get_value('products'):
            if sw_type := emr_props.get_value_silent('products', sku, 'attributes', 'softwareType'):
                if sw_type == 'EMR':
                    sku_to_instance_type[sku] = emr_props.get_value('products', sku, 'attributes', 'instanceType')
        self.__load_instance_type_price_by_sku(prop_key, emr_props, sku_to_instance_type)

    def _load_instance_types_ec2(self, prop_key: str, catalog_file: str):
        ec2_props = JSONPropertiesContainer(catalog_file)
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
        self.__load_instance_type_price_by_sku(prop_key, ec2_props, ec2_sku_to_instance_type)

    def get_value(self, *key_strs):
        return get_elem_from_dict(self.props, key_strs)

    def get_value_silent(self, *key_strs):
        return get_elem_non_safe(self.props, key_strs)

    def __post_init__(self):
        for catalog_k in self.catalog_files:
            func_name = f'_load_instance_types_{catalog_k}'
            if hasattr(self, func_name):
                if callable(func_obj := getattr(self, func_name)):
                    func_obj(catalog_k, self.catalog_files.get(catalog_k))


@dataclass
class EMREc2PriceProvider(PriceProvider):
    """
    Provide costs of EMR running on Ec2 instances
    """
    name = 'Emr-Ec2'

    def _generate_cache_files(self):
        aws_url_base = 'https://pricing.us-east-1.amazonaws.com'
        emr_region_ind_url = f'{aws_url_base}/offers/v1.0/aws/ElasticMapReduce/current/region_index.json'
        emr_region_resp = requests.get(emr_region_ind_url, timeout=60)
        emr_region_relative_url = emr_region_resp.json()['regions'][self.region]['currentVersionUrl']
        ec2_region_ind_url = f'{aws_url_base}/offers/v1.0/aws/AmazonEC2/current/region_index.json'
        ec2_region_resp = requests.get(ec2_region_ind_url, timeout=60)
        ec2_region_relative_url = ec2_region_resp.json()['regions'][self.region]['currentVersionUrl']
        self.resource_urls = {
            'emr': f'{aws_url_base}{emr_region_relative_url}',
            'ec2': f'{aws_url_base}{ec2_region_relative_url}'
        }
        super()._generate_cache_files()

    def _process_resource_configs(self):
        def get_cache_file_path(comp,  region) -> str:
            file_name = f'aws_ec2_catalog_{comp}_{region}.json'
            # get file from cache folder
            return FSUtil.build_path(self.cache_directory, file_name)
        self.cache_files = {
            'emr': get_cache_file_path('emr', self.region),
            'ec2': get_cache_file_path('ec2', self.region)}

    def _create_catalogs(self):
        self.catalogs = {'aws': AWSCatalogContainer(self.cache_files)}
