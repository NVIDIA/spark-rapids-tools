#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# SPDX-FileCopyrightText: Copyright (c) 2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----
"""Spark RAPIDS speedup factor generation script"""

import argparse
import os

import pandas as pd

parser = argparse.ArgumentParser(description="Speedup Factor Analysis")
parser.add_argument("--cpu", type=str, help="Directory of CPU profiler logs", required=True)
parser.add_argument("--gpu", type=str, help="Directory of GPU profiler logs", required=True)
parser.add_argument("--verbose", action="store_true", help="flag to generate full verbose output for logging raw node results")
parser.add_argument("--chdir", action="store_true", help="flag to change to work dir that's the script located")
args = parser.parse_args()

cpu_dir = args.cpu
gpu_dir = args.gpu
verbose = args.verbose

cpu_stage_log = {}
gpu_stage_log = {}

if args.chdir:
    # Change to work dir that's the script located
    os.chdir(os.path.dirname(__file__))

# CPU log parsing
for app in os.listdir(cpu_dir):

    # - figure out query from application_info.csv
    app_info = pd.read_csv(cpu_dir + "/" + app + "/application_information.csv")
    app_name = app_info.loc[0]["appName"]
    cpu_stage_log[app_name] = {}

    # - load wholestagecodegen_mapping.csv into a dictionary for lookups (CPU only)
    mapping_info = pd.read_csv(cpu_dir + "/" + app + "/wholestagecodegen_mapping.csv")
    mapping_info = mapping_info.groupby(['SQL Node'])['Child Node'].apply(','.join).reset_index()

    # - process sql_plan_metrics_for_application.csv
    #   - load in "duration" (CPU) or "op time" (GPU)
    #   - replace WholeStageCodegen (CPU only) with list of operators from mapping lookup file
    #     - mapping_info.parent = sql_times.nodeName
    cpu_sql_info = pd.read_csv(cpu_dir + "/" + app + "/sql_plan_metrics_for_application.csv")
    cpu_sql_times = cpu_sql_info[cpu_sql_info["name"] == "duration"]
    cpu_sql_combined = cpu_sql_times.set_index('nodeName').join(mapping_info.set_index('SQL Node'), how='left')

    cpu_stage_info = pd.read_csv(cpu_dir + "/" + app + "/sql_to_stage_information.csv")
    cpu_stage_times = cpu_stage_info[['Stage Duration', 'SQL Nodes(IDs)']]

    cpu_stage_times_df = cpu_stage_times.dropna()
    for index, row in cpu_stage_times_df.iterrows():
        operators = str(row['SQL Nodes(IDs)']).split(',')
        duration = 0.0
        if "WholeStageCodegen" in operators:
            duration = row['Stage Duration']/(len(operators)-1)
        else:
            duration = row['Stage Duration']/(len(operators))
        for operator in operators:
            # handle WholeStageCodegen node
            if "WholeStageCodegen" in operator:
                continue

            op_key = operator.split('(')[0]
            if op_key in cpu_stage_log[app_name]:
                cpu_stage_log[app_name][op_key] = cpu_stage_log[app_name][op_key] + duration
            else:
                cpu_stage_log[app_name][op_key] = duration

# GPU log parsing
for app in os.listdir(gpu_dir):

    # - figure out query from application_info.csv
    app_info = pd.read_csv(gpu_dir + "/" + app + "/application_information.csv")
    app_name = app_info.loc[0]["appName"]
    gpu_stage_log[app_name] = {}

    # - process sql_plan_metrics_for_application.csv
    #   - load in "duration" (CPU) or "op time" (GPU)
    #     - mapping_info.parent = sql_times.nodeName
    gpu_sql_info = pd.read_csv(gpu_dir + "/" + app + "/sql_plan_metrics_for_application.csv")
    gpu_sql_times = gpu_sql_info[gpu_sql_info["name"] == "op time"]

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

# Sum up SQL operators for each
stage_totals = {}
cpu_stage_total = 0.0
gpu_stage_total = 0.0

for app_key in cpu_stage_log:
    for op_key in cpu_stage_log[app_key]:
        if op_key not in stage_totals:
            stage_totals[op_key] = cpu_stage_log[app_key][op_key]
        else:
            stage_totals[op_key] = stage_totals[op_key] + cpu_stage_log[app_key][op_key]
        cpu_stage_total = cpu_stage_total + cpu_stage_log[app_key][op_key]


for app_key in gpu_stage_log:
    for op_key in gpu_stage_log[app_key]:
        if op_key not in stage_totals:
            stage_totals[op_key] = gpu_stage_log[app_key][op_key]
        else:
            stage_totals[op_key] = stage_totals[op_key] + gpu_stage_log[app_key][op_key]
        gpu_stage_total = gpu_stage_total + gpu_stage_log[app_key][op_key]

# Print out node metrics (if verbose)
if verbose:
    print("# Operator metrics ")
    for key in stage_totals:
        print(key + "," + str(stage_totals[key]))
    print("CPU Total," + str(cpu_stage_total))
    print("GPU Total," + str(gpu_stage_total))

# Print out speedup factors
print("# Speedup Factors ")
print("FilterExec," + str(round(stage_totals['Filter'] / stage_totals['GpuFilter'], 2)))
print("SortExec," + str(round(stage_totals['SortMergeJoin'] / stage_totals['GpuShuffledHashJoin'], 2)))
print("BroadcastHashJoinExec," + str(round(stage_totals['BroadcastHashJoin'] / stage_totals['GpuBroadcastHashJoin'], 2)))
print("ShuffleExchangeExec," + str(round(stage_totals['Exchange'] / stage_totals['GpuColumnarExchange'], 2)))
print("HashAggregateExec," + str(round(stage_totals['HashAggregate'] / stage_totals['GpuHashAggregate'], 2)))
print("SortMergeJoinExec," + str(round((stage_totals['SortMergeJoin']+stage_totals['Sort']) / (stage_totals['GpuShuffledHashJoin']+stage_totals['GpuSort']), 2)))
