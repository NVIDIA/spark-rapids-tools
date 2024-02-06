#!/usr/bin/env python3
# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

"""Spark RAPIDS speedup factor generation script"""

import argparse
import os

import pandas as pd


def list_directories(root_name):
    for root, dirs, files in os.walk(root_name):
        return dirs
    return []


parser = argparse.ArgumentParser(description="Speedup Factor Analysis")
parser.add_argument("--cpu", type=str, help="Directory of CPU profiler logs", required=True)
parser.add_argument("--gpu", type=str, help="Directory of GPU profiler logs", required=True)
parser.add_argument("--output", type=str, help="Filename for custom speedup factors", required=True)
parser.add_argument("--verbose", action="store_true", help="flag to generate full verbose output for logging raw node results")
parser.add_argument("--chdir", action="store_true", help="flag to change to work dir that's the script located")
args = parser.parse_args()

cpu_dir = args.cpu
gpu_dir = args.gpu
output = args.output
verbose = args.verbose

cpu_stage_log = {}
gpu_stage_log = {}
cpu_duration = 0.0
gpu_duration = 0.0

min_speedup = 1.0

if args.chdir:
    # Change to work dir that's the script located
    os.chdir(os.path.dirname(__file__))

# CPU log parsing
for app in list_directories(cpu_dir):

    # - figure out query from application_info.csv
    app_info = pd.read_csv(cpu_dir + "/" + app + "/application_information.csv")
    app_name = app_info.loc[0]["appName"]
    cpu_duration = cpu_duration + app_info.loc[0]["duration"]
    cpu_stage_log[app_name] = {}

    # - load wholestagecodegen_mapping.csv into a dictionary for lookups (CPU only)
    mapping_info = pd.read_csv(cpu_dir + "/" + app + "/wholestagecodegen_mapping.csv")
    mapping_info = mapping_info.groupby(['SQL Node'])['Child Node'].apply(','.join).reset_index()

    # - process sql_plan_metrics_for_application.csv
    #   - load in "duration" (CPU)
    #   - replace WholeStageCodegen (CPU only) with list of operators from mapping lookup file
    #     - mapping_info.parent = sql_times.nodeName
    cpu_sql_info = pd.read_csv(cpu_dir + "/" + app + "/sql_plan_metrics_for_application.csv")
    cpu_sql_times = cpu_sql_info[cpu_sql_info["name"] == "duration"]
    cpu_sql_combined = cpu_sql_times.set_index('nodeName').join(mapping_info.set_index('SQL Node'), how='left')

    #  - parse WholeStageCodegen durations with child node mapping
    cpu_sql_times_df = cpu_sql_combined[['Child Node', 'total']]

    for index, row in cpu_sql_times_df.iterrows():
        operators = str(row['Child Node']).split(',')
        duration = row['total']/len(operators)/1000.0
        for operator in operators:
            if operator in cpu_stage_log[app_name]:
                cpu_stage_log[app_name][operator] = cpu_stage_log[app_name][operator] + duration
            else:
                cpu_stage_log[app_name][operator] = duration

    # - parse top-level execs from sql_to_stage_information.csv
    cpu_stage_info = pd.read_csv(cpu_dir + "/" + app + "/sql_to_stage_information.csv")
    cpu_stage_times = cpu_stage_info[['Stage Duration', 'SQL Nodes(IDs)']]
    cpu_stage_times_df = cpu_stage_times.dropna()

    for index, row in cpu_stage_times_df.iterrows():
        node_list = str(row['SQL Nodes(IDs)'])
        operators = node_list.split(',')
        duration = row['Stage Duration']/(len(operators)-node_list.count("WholeStageCodegen"))

        for operator in operators:
            if "WholeStageCodegen" in operator:
                continue

            op_key = operator.split('(')[0]
            if op_key in cpu_stage_log[app_name]:
                cpu_stage_log[app_name][op_key] = cpu_stage_log[app_name][op_key] + duration
            else:
                cpu_stage_log[app_name][op_key] = duration

# GPU log parsing
for app in list_directories(gpu_dir):

    # - figure out query from application_info.csv
    app_info = pd.read_csv(gpu_dir + "/" + app + "/application_information.csv")
    app_name = app_info.loc[0]["appName"]
    gpu_duration = gpu_duration + app_info.loc[0]["duration"]
    gpu_stage_log[app_name] = {}

    # - process sql_to_stage_information.csv to get stage durations
    # - split up duration by operators listed in each stage
    gpu_stage_info = pd.read_csv(gpu_dir + "/" + app + "/sql_to_stage_information.csv")
    gpu_stage_times = gpu_stage_info[['Stage Duration', 'SQL Nodes(IDs)']]

    for index, row in gpu_stage_times.iterrows():
        operators = str(row['SQL Nodes(IDs)']).split(',')
        duration = row['Stage Duration']/len(operators)
        for operator in operators:
            op_key = operator.split('(')[0]
            if op_key in gpu_stage_log[app_name]:
                gpu_stage_log[app_name][op_key] = gpu_stage_log[app_name][op_key] + duration
            else:
                gpu_stage_log[app_name][op_key] = duration

cpu_stage_totals = {}
gpu_stage_totals = {}
cpu_stage_total = 0.0
gpu_stage_total = 0.0

# Sum up SQL operators for each operator found in CPU and GPU
for app_key in cpu_stage_log:
    for op_key in cpu_stage_log[app_key]:
        if op_key not in cpu_stage_totals:
            cpu_stage_totals[op_key] = cpu_stage_log[app_key][op_key]
        else:
            cpu_stage_totals[op_key] = cpu_stage_totals[op_key] + cpu_stage_log[app_key][op_key]
        cpu_stage_total = cpu_stage_total + cpu_stage_log[app_key][op_key]

for app_key in gpu_stage_log:
    for op_key in gpu_stage_log[app_key]:
        if op_key not in gpu_stage_totals:
            gpu_stage_totals[op_key] = gpu_stage_log[app_key][op_key]
        else:
            gpu_stage_totals[op_key] = gpu_stage_totals[op_key] + gpu_stage_log[app_key][op_key]
        gpu_stage_total = gpu_stage_total + gpu_stage_log[app_key][op_key]

# Create dictionary of execs where speedup factors can be calculated
scores_dict = {}

# Scan operators
if 'Scan parquet ' in cpu_stage_totals and 'GpuScan parquet ' in gpu_stage_totals:
    scores_dict["BatchScanExec"] = str(round(cpu_stage_totals['Scan parquet '] / gpu_stage_totals['GpuScan parquet '], 2))
    scores_dict["FileSourceScanExec"] = str(round(cpu_stage_totals['Scan parquet '] / gpu_stage_totals['GpuScan parquet '], 2))
if 'Scan orc ' in cpu_stage_totals and 'GpuScan orc ' in gpu_stage_totals:
    scores_dict["BatchScanExec"] = str(round(cpu_stage_totals['Scan orc '] / gpu_stage_totals['GpuScan orc '], 2))
    scores_dict["FileSourceScanExec"] = str(round(cpu_stage_totals['Scan orc '] / gpu_stage_totals['GpuScan orc '], 2))

# Other operators
if 'Expand' in cpu_stage_totals and 'GpuExpand' in gpu_stage_totals:
    scores_dict["ExpandExec"] = str(round(cpu_stage_totals['Expand'] / gpu_stage_totals['GpuExpand'], 2))
if 'CartesianProduct' in cpu_stage_totals and 'GpuCartesianProduct' in gpu_stage_totals:
    scores_dict["CartesianProductExec"] = str(round(cpu_stage_totals['CartesianProduct'] / gpu_stage_totals['GpuCartesianProduct'], 2))
if 'Filter' in cpu_stage_totals and 'GpuFilter' in gpu_stage_totals:
    scores_dict["FilterExec"] = str(round(cpu_stage_totals['Filter'] / gpu_stage_totals['GpuFilter'], 2))
if 'SortMergeJoin' in cpu_stage_totals and 'GpuShuffledHashJoin' in gpu_stage_totals:
    scores_dict["SortMergeJoinExec"] = str(round(cpu_stage_totals['SortMergeJoin'] / gpu_stage_totals['GpuShuffledHashJoin'], 2))
if 'BroadcastHashJoin' in cpu_stage_totals and 'GpuBroadcastHashJoin' in gpu_stage_totals:
    scores_dict["BroadcastHashJoinExec"] = str(round(cpu_stage_totals['BroadcastHashJoin'] / gpu_stage_totals['GpuBroadcastHashJoin'], 2))
if 'Exchange' in cpu_stage_totals and 'GpuColumnarExchange' in gpu_stage_totals:
    scores_dict["ShuffleExchangeExec"] = str(round(cpu_stage_totals['Exchange'] / gpu_stage_totals['GpuColumnarExchange'], 2))
if 'HashAggregate' in cpu_stage_totals and 'GpuHashAggregate' in gpu_stage_totals:
    scores_dict["HashAggregateExec"] = str(round(cpu_stage_totals['HashAggregate'] / gpu_stage_totals['GpuHashAggregate'], 2))
    scores_dict["ObjectHashAggregateExec"] = str(round(cpu_stage_totals['HashAggregate'] / gpu_stage_totals['GpuHashAggregate'], 2))
    scores_dict["SortAggregateExec"] = str(round(cpu_stage_totals['HashAggregate'] / gpu_stage_totals['GpuHashAggregate'], 2))
if 'TakeOrderedAndProject' in cpu_stage_totals and 'GpuTopN' in gpu_stage_totals:
    scores_dict["TakeOrderedAndProjectExec"] = str(round(cpu_stage_totals['TakeOrderedAndProject'] / gpu_stage_totals['GpuTopN'], 2))
if 'BroadcastNestedLoopJoin' in cpu_stage_totals and 'GpuBroadcastNestedLoopJoin' in gpu_stage_totals:
    scores_dict["BroadcastNestedLoopJoinExec"] = str(round(cpu_stage_totals['BroadcastNestedLoopJoin'] / gpu_stage_totals['GpuBroadcastNestedLoopJoin'], 2))

# Set minimum to 1.0 for speedup factors
for key in scores_dict:
    if float(scores_dict[key]) < min_speedup:
        scores_dict[key] = f"{min_speedup}"

# Set overall speedup for default value for execs not in logs
overall_speedup = str(max(min_speedup, round(cpu_duration/gpu_duration, 2)))

# Print out node metrics (if verbose)
if verbose:
    print("# CPU Operator Metrics")
    for key in cpu_stage_totals:
        print(key + " = " + str(cpu_stage_totals[key]))
    print("# GPU Operator Metrics")
    for key in gpu_stage_totals:
        print(key + " = " + str(gpu_stage_totals[key]))
    print("# Summary Metrics")
    print("CPU Total = " + str(cpu_stage_total))
    print("GPU Total = " + str(gpu_stage_total))
    print("Overall speedup = " + overall_speedup)

    # Print out individual exec speedup factors
    print("# Speedup Factors ")
    for key in scores_dict:
        print(f"{key} = {scores_dict[key]}")

# Load in list of operators and set initial values to default speedup
scores_df = pd.read_csv("operatorsList.csv")
scores_df["Score"] = overall_speedup

# Update operators that are found in benchmark
for key in scores_dict:
    scores_df.loc[scores_df['CPUOperator'] == key, 'Score'] = scores_dict[key]

# Add in hard-coded defaults
defaults_df = pd.read_csv("defaultScores.csv")

# Generate output CSV file
final_df = pd.concat([scores_df, defaults_df])
final_df.to_csv(output, index=False)
