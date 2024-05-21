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

1. Take the union of the supported CSV files from the plugin (since plugin has supported data for each spark-version).
2. Use the `custom_configs_file` to override the merged CSV files.
3. Generate a `operators_plugin_sync_report.txt` for reviewers to understand the changes made in the plugin side. Note:
   1. For each new data source/exec/expression from the plugin, a new entry is added to `override_supported_configs.json` to mark the `Supported` column as `TNEW`
   2. Rows marked as "is removed" in the report will be preserved in the final output.
   3. The `Notes` column for rows with `S` for `Supported` will be updated to `None` in the final output.
4. Generate a `new_operators.txt` file to store all the new exec/expression from the plugin side.
5. Write the final results to `output_directory`.

More details could be found in the documentation in `process_supported_files.py`.

## Next Steps

After running the python script to generate the new supported CSV files to replace the existing ones in tools, we need to:
1. Parse and test the execs or expressions added in the plugin side, which has support level of `TNEW`
2. Once done with the above step, we can remove the corresponding entries in `override_supported_configs.json`


# Sync Up Plugin Supported CSV Files


## Purpose

The python script `sync_operator_scores.py` is used for appending the new operators to the operator score files in the tools repo. This is useful because without it the engineers need to manually add the scores to a list of files, which is not efficient.

## Prerequisites

python >= 3.8

## Running the Sync Up Script
```
python sync_operator_scores.py new_operators operator_score_dir [--new-score score]
```

The script take three parameters:
- `new_operators`: required, a text file of the new operators, separated by \newline
- `operator_score_dir`: required, path to directory with operator score files
- `--new-score`: optional, a score for the new operators, default to 1.5
