# Sync Up Plugin Supported CSV Files

## Purpose

The python script `process_supported_files.py` is used for syncing up with plugin's supported CSV files: `supportedDataSource.csv`, `supportedExecs.csv`, `supportedExprs.csv`. This is necessary because the plugin could add, remove, or modify supported operator information from time to time. The tools needs to keep track of the most updated supported CSV files from the plugin side.

## Prerequisites

1. python >= 3.8
2. Necessary Python dependencies: `pandas`, `numpy`

## Running the Sync Up Script
```
python process_supported_files.py <path_to_plugin_generated_csv_files> --configs <custom_configs_file> --tools-csv <path_to_tools_existing_csv_files> --output <output_directory>
```

The high-level process of the above command is as follows:

1. Union the supported CSV files from the plugin (since plugin has supported data for each spark-version)
2. Use the `custom_configs_file` to override the merged CSV files
3. Genrate a `report.txt` for reviewers to understand the changes made in the plugin side
   1. For new data source/exec/expression from the plugin, a new entry is added to `override_supported_configs.json` for the `TNEW` label
4. Write the final results to `output_directory`

More details could be found in the documentation in `process_supported_files.py`.

## Next Steps

After running the python script to generate the new supported CSV files to replace the existing ones in tools, we need to:
1. Parse and test the execs or expressions added in the plugin side, which has support level of `TNEW`
2. Once done with the above step, we can remove the corresponding entry in `override_supported_configs.json`
