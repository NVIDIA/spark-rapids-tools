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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Description:
    This Python script goes over all directories of different spark versions under the
    spark-rapids/tools/generated_files folder and contruct unions of the supported csv
    files (supportedDataSource.csv, supportedExecs.csv, and supportedExprs.csv).

Usage:
    python process_supported_files.py path_to_generated_files_dir
"""


import os
import argparse
import pandas as pd
from enum import IntEnum
import logging


"""
IntEnum Class for support level types and easy comparison
"""
class SupportLevel(IntEnum):
    NS = 1
    NA = 2
    PS = 3
    S = 4
    CO = 5


"""
Check if input is in SupportLevel class
"""
def is_support_level(name):
    return name in SupportLevel.__members__


"""
Compare two entries in the CSV file DFs:
  1) If they are both SupportLevel types, compare using their interger values
  2) If not, compare their length (assuming strings with greater length contain more info)
"""
def is_greater(elem1, elem2):
    # print("elem1", elem1)
    # print("elem2", elem2)
    if is_support_level(elem1) and is_support_level(elem2): 
        return SupportLevel[elem1] > SupportLevel[elem2]
    else:
        return len(elem1) > len(elem2)


"""
Given two df rows and a list of keys, check if the rows have the same values for all keys.
"""
def check_df_rows(row1, row2, keys):
    for key in keys:
        if row1[key] != row2[key]:
            return False
    return True


"""
Searches for file_name in all spark-version folders and unifies the CSV files into a single
Dataframe. The rule for union is that:
  1) If a data source/exec/expr exists in any spark version, include in the final output.
  2) For each value in a row, escalate the support level: CO > S > PS > NA > NS.

Parameters:
root_dir: The directory that contains folders of CSV data for every spark version.
          > root_dir
            > 311
            ...
            > 350
              > supportedExecs.csv
              > supportedExprs.csv
              ...
file_name: CSV file to process.
key_names: Tuple of two column names which uniquely identifies a row in the CSV file.
"""
def unify_all_files(root_dir, file_name, key_names):
    final_df = None 

    for dir_name in os.listdir(root_dir): # List entries in root_dir
        if os.path.isdir(os.path.join(root_dir, dir_name)):
            csv_file_path = os.path.join(root_dir, dir_name, file_name)
            cur_df = pd.read_csv(csv_file_path, keep_default_na=False)

            if final_df is None:
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


"""
Post process the union of supported_exprs.csv file due to a difference in "PromotePrecision" data
between plugin and tools repos.
"""
def post_process_supported_exprs_csv(df):
    for cur_idx, cur_row in df.iterrows():
        if cur_row["Expression"] == "PromotePrecision":
            df.at[cur_idx, "SQL Func"] = "`promote_precision`"
    return df


"""
Main function of the script.

Parameters:
args: Namespace containing the command-line arguments.
"""
def main(args):
    genrated_files_dir = args.path

    dataSourceUnionDF = unify_all_files(genrated_files_dir, "supportedDataSource.csv", ("Format", "Direction"))
    dataSourceUnionDF.to_csv('unionSupportedDataSource.csv', index=False)
    execsUnionDF = unify_all_files(genrated_files_dir, "supportedExecs.csv", ("Exec", "Params"))
    execsUnionDF.to_csv('unionSupportedExecs.csv', index=False)
    exprsUnionDF = unify_all_files(genrated_files_dir, "supportedExprs.csv", ("Expression", "Context", "Params"))
    exprsUnionDF = post_process_supported_exprs_csv(exprsUnionDF)
    exprsUnionDF.to_csv('unionSupportedExprs.csv', index=False)

    print(genrated_files_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Path to genrated_files directory.")
    args = parser.parse_args()
    
    main(args)
