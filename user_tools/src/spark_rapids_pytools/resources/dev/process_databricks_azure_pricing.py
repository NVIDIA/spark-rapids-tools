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

"""Python script used to process Databricks Azure instance pricing information."""

import json

# We use this Python script to process Databricks Azure instance pricing information
# and store the results in a file `premium-databricks-azure-catalog.json`, used by the
# Databricks Azure user tools.
# Follow the instructions below:
#   1. Go to https://azure.microsoft.com/en-us/pricing/details/databricks/ and select
#      the prices for `Jobs Cmpute` Workload, `Premium` Tier, `West US 2` Region,
#      `United States -Dollar ($) USD` Currency, and Display pricing by `Hour`.
#   2. Copy the prices into `databricks-azure-price-jobs-compute-premium-westus2-raw.txt`,
#      under the same directory as this script, the file is already included under `/dev`
#   3. Run the script:
#     `python process_databricks_azure_pricing.py`
#   4. The output is a file named `premium-databricks-azure-catalog.json` under the same
#      directory

with open('databricks-azure-price-jobs-compute-premium-westus2-raw.txt', 'r', encoding='utf-8') as f:
    all_lines = f.read().splitlines()

instances_dict = {}

for _, line in enumerate(all_lines):
    cur_dict = {}
    cur_line = line.split()
    gb_idx = cur_line.index('GiB')
    cur_dict['RAMinMB'] = 1024 * int(float(cur_line[gb_idx - 1]))
    vCPUs = cur_line[gb_idx - 2]
    cur_dict['vCPUs'] = int(vCPUs)
    instance_name = '_'.join(cur_line[:gb_idx - 2])
    cur_dict['Instance'] = instance_name
    cur_dict['DBUCount'] = float(cur_line[gb_idx + 1])
    DBU_price_per_hour = cur_line[gb_idx + 2].split('$')[1].split('/')[0]
    cur_dict['DBUPricePerHour'] = float(DBU_price_per_hour)
    try:
        total_price_per_hour = cur_line[gb_idx + 3].split('$')[1].split('/')[0]
        total_price_per_hour_float = float(total_price_per_hour)
    except RuntimeError:  # price could be 'N/A'
        total_price_per_hour_float = -1.0
    cur_dict['TotalPricePerHour'] = total_price_per_hour_float
    instances_dict[instance_name] = cur_dict

final_dict = {'Jobs Compute': {'Instances': instances_dict}}
with open('./premium-databricks-azure-catalog.json', 'w', encoding='utf-8') as output_file:
    json.dump(final_dict, output_file, indent=2)
