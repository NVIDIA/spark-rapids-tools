"""Python script used to process Databricks Azure instance pricing information."""

import json

# pricing info: https://azure.microsoft.com/en-us/pricing/details/databricks/
with open('databricks_azure_price_raw_westus2.txt', 'r', encoding='utf-8') as f:
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
