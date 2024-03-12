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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Description:
    This Python script goes over all directories of different spark versions under the
    spark-rapids/tools/generated_files folder, contructs unions of the supported CSV
    files (supportedDataSource.csv, supportedExecs.csv, and supportedExprs.csv) and
    writes the results to new CSV files.

Dependencies:
    - numpy >= 1.23.3
    - pandas >= 2.0.3

Usage:
    python process_supported_files.py generated_files_path [--configs configs_file] [--output output_directory]
"""


import argparse
import json
import logging
import os
from enum import IntEnum

import pandas as pd


class SupportLevel(IntEnum):
    """
    IntEnum Class for support level types to facilitate easy comparison.

    Enum members:
    - NS: Not Supported.
    - NA: Not Applicable.
    - PS: Partially Supported.
    - S: Supported.
    - CO: ???.
    """
    NS = 1
    NA = 2
    PS = 3
    S = 4
    CO = 5


def is_support_level(elem):
    """
    Check if elem is a valid name of a SupportLevel member.
    """
    return elem in SupportLevel.__members__


def is_greater(elem1, elem2):
    """
    Compare two entries, which might be names of SupportLevel members or strings.

    1) If they are both valid names of SupportLevel members,
       compare using their integer values.
    2) If not, compare their length (assuming strings with greater length contain more info).
    """
    if is_support_level(elem1) and is_support_level(elem2): 
        return SupportLevel[elem1] > SupportLevel[elem2]
    else:
        return len(elem1) > len(elem2)


def check_df_rows(row1, row2, keys):
    """
    Given two DataFrame rows (pandas Series) and a list of keys (column names),
    check if the rows have the same values for all specified keys.
    """
    for key in keys:
        if row1[key] != row2[key]:
            return False
    return True


def unify_all_files(root_dir, file_name, key_names):
    """
    Search for file_name in all spark-version folders and unify the CSV files into a
    single Dataframe, using the following rules:
    1) If a data source/exec/expr exists in any spark version, include in the final output.
    2) For each value in a row, escalate the support level: CO > S > PS > NA > NS.

    Parameters:
    - root_dir: The directory containing folders of CSV data for every spark version.
                > root_dir
                    > 311
                    ...
                    > 350
                        > supportedExecs.csv
                        > supportedExprs.csv
                        ...
    - file_name: CSV file to process.
    - key_names: List of column names which uniquely identifies a row in the CSV file.
    """
    final_df = pd.DataFrame()

    for dir_name in os.listdir(root_dir): # List entries in root_dir
        if os.path.isdir(os.path.join(root_dir, dir_name)):
            csv_file_path = os.path.join(root_dir, dir_name, file_name)
            cur_df = pd.read_csv(csv_file_path, keep_default_na=False)

            if final_df.empty:
                final_df = pd.DataFrame(columns=cur_df.columns.tolist())
            
            # expand final_df if cur_df has more columns
            for col_name in cur_df.columns:
                if not col_name in final_df.columns:
                    logging.debug(f"Expanding final_df with new column name: {col_name}")
                    final_df[col_name] = ["NS" for _ in range(final_df.shape[0])]
            
            # iterate through every row in the current df and update the final df correspondingly
            for _, cur_row in cur_df.iterrows():
                # expand current_row if final_df has more columns
                for col_name in final_df.columns:
                    if not col_name in cur_row:
                        logging.debug(f"Expanding cur_row with entry: ({col_name}, NS)")
                        cur_row.loc[col_name] = "NS"
   
                updated_flag = False
                for final_idx, final_row in final_df.iterrows():
                    # current row exists in final df, unify them and update final df
                    if check_df_rows(cur_row, final_row, key_names):
                        updated_flag = True
                        for col_name in final_df.columns:
                            # if cur_row[col_name] has higher support level, update final_row[col_name]
                            if (not col_name in key_names) and is_greater(cur_row[col_name], final_row[col_name]):
                                logging.debug(f"Updating final_df at ({final_idx}, {col_name}) = {cur_row[col_name]}")
                                final_df.at[final_idx, col_name] = cur_row[col_name]
                # current_row does not exist in final df, append it
                if not updated_flag:
                    logging.debug("Appending row to final_df: {cur_row.values}")
                    final_df = pd.concat([final_df, cur_row.to_frame().T], ignore_index=True)
            
    logging.debug(f"final_df = {final_df}")
    return final_df


def check_override(entry, row_data, keys):
    """
    Given a JSON entry and a DataFrame row (pandas Series) and a list of keys,
    check if the entry and row have the same values for all specified keys.
    """
    for key in keys:
        if not entry[key] == row_data[key]:
            return False
    return True


def override_supported_configs(json_data, file_name, df, keys):
    """
    Override dataframe df with input json_data.

    Parameters:
    - json_data: Data in JSON format to explicitly override input DataFrame.
    - file_name: Key within json_data containing override information.
    - df: Pandas DataFrame to be modified.
    - keys: A list of column names which identifies a row in df.

    Returns:
    - The modified DataFrame.

    Example JSON input:
    json_data = {
        "supportedExprs.csv": [
            {
                "Expression": "PromotePrecision",
                "Context": "project",
                "Params": "input",
                "override": [{"key": "SQL Func", "value": "`promote_precision`"}]
            }
        ]
    }
    """
    if file_name in json_data:
        for entry in json_data[file_name]:
            for idx, row_data in df.iterrows():
                if check_override(entry, row_data, keys):
                    for config in entry["override"]:
                        df.at[idx, config["key"]] = config["value"]
    return df


def main(args):
    """
    Main function of the script.

    Parameters:
    args: Namespace containing the command-line arguments
    """
    genrated_files_dir = args.path
    override_configs_file = args.configs
    output_dir = args.output

    # generate the union of supported files as pandas dataframe
    data_source_union_df = unify_all_files(genrated_files_dir, "supportedDataSource.csv", ["Format", "Direction"])
    execs_union_df = unify_all_files(genrated_files_dir, "supportedExecs.csv", ["Exec", "Params"])
    exprs_union_df = unify_all_files(genrated_files_dir, "supportedExprs.csv", ["Expression", "Context", "Params"])

    # post-process the dataframes to override customed configs
    if override_configs_file:
        with open(override_configs_file, 'r') as f:
                override_configs = json.load(f)
        data_source_union_df = override_supported_configs(override_configs, "supportedDataSource.csv", data_source_union_df, ["Format", "Direction"])
        execs_union_df = override_supported_configs(override_configs, "supportedExecs.csv", execs_union_df, ["Exec", "Params"])
        exprs_union_df = override_supported_configs(override_configs, "supportedExprs.csv", exprs_union_df, ["Expression", "Context", "Params"])

    # write the result dataframes to output CSV files
    data_source_union_df.to_csv(f"{output_dir}/supportedDataSource.csv", index=False)
    execs_union_df.to_csv(f"{output_dir}/supportedExecs.csv", index=False)
    exprs_union_df.to_csv(f"{output_dir}/supportedExprs.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Path to genrated_files directory.")
    parser.add_argument('--configs', type=str, help='Path to configs file for overriding current data.')
    parser.add_argument('--output', type=str, help='Path to output directory.', default='.')

    args = parser.parse_args()
    main(args)
