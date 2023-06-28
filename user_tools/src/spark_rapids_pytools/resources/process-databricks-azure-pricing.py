import json

# pricing info: https://azure.microsoft.com/en-us/pricing/details/databricks/ 
f = open("databricks_azure_price_raw_westus2.txt", "r")
all_lines = f.read().splitlines()
instances_dict = {}

for i in range(len(all_lines)):
    cur_dict = {}
    cur_line = all_lines[i].split()
    gb_idx = cur_line.index('GiB')
    cur_dict['RAMinMB'] = 1024 * int(float(cur_line[gb_idx - 1]))
    vCPUs = cur_line[gb_idx - 2]
    cur_dict['vCPUs'] = int(vCPUs)
    instance_name = "_".join(cur_line[:gb_idx - 2])
    cur_dict['Instance'] = instance_name
    cur_dict['DBUCount'] = float(cur_line[gb_idx + 1])
    DBUPricePerHour = cur_line[gb_idx + 2].split('$')[1].split('/')[0]
    cur_dict['DBUPricePerHour'] = float(DBUPricePerHour)
    try:
        TotalPricePerHour = cur_line[gb_idx + 3].split('$')[1].split('/')[0]
        TotalPricePerHourFloat = float(TotalPricePerHour)
    except:  # price could be 'N/A'
        TotalPricePerHourFloat = -1.0
    cur_dict['TotalPricePerHour'] = TotalPricePerHourFloat
    instances_dict[instance_name] = cur_dict

output_file = open("./premium-databricks-azure-catalog.json", "w")
final_dict = {"Jobs Compute" : {'Instances': instances_dict}}
json.dump(final_dict, output_file, indent = 2)