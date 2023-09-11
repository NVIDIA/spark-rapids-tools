#!/usr/bin/env python3
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

"""Spark RAPIDS speedup factor validation script"""

import argparse
import os
import glob
import subprocess

import pandas as pd
from tabulate import tabulate

parser = argparse.ArgumentParser(description="Speedup Factor Validation")
parser.add_argument("--cpu_log", type=str, help="Directory of CPU event log(s)", required=True)
parser.add_argument("--gpu_log", type=str, help="Directory of GPU event log(s)", required=True)
parser.add_argument("--output", type=str, help="Output folder for storing logs", required=True)
parser.add_argument("--speedups", type=str, help="Custom speedup factor file")
parser.add_argument("--cpu_profile", type=str, help="Directory of CPU profiler log(s)")
parser.add_argument("--gpu_profile", type=str, help="Directory of GPU profiler log(s)")
parser.add_argument("--jar", type=str, help="Custom tools jar")
parser.add_argument("--verbose", action="store_true", help="flag to generate full verbose output for logging raw node results")
args = parser.parse_args()

cpu_log = args.cpu_log
gpu_log = args.gpu_log
cpu_profile = args.cpu_profile
gpu_profile = args.gpu_profile
output = args.output
speedups = args.speedups
jar = args.jar
verbose = args.verbose

print(f"Output folder = {output}")
print(f"CPU event log = {cpu_log}")
print(f"GPU event log = {gpu_log}")

subprocess.run(f"rm -rf {output}", shell=True)

speedups_arg = ""
if speedups is not None:
    speedups_arg = f"--speedup-factor-file {speedups}"
else:
    speedups_arg = f"--speedup-factor-file {output}/generatedScores.csv"

jar_arg = ""
if jar is not None:
    jar_arg = f"--tools_jar {jar}"

# Generate speedup factors

### run GPU profiler if needed
gpu_profile_dir = ""
if gpu_profile is not None:
    gpu_profile_dir = gpu_profile
else:
    gpu_profile_dir = f"{output}/gpu_profile"
    subprocess.run(f"spark_rapids_user_tools onprem profiling --csv {jar_arg} --local_folder {gpu_profile_dir} --eventlogs {gpu_log}", shell=True)

if speedups is None:
    ### run CPU profiler if needed
    cpu_profile_dir = ""
    if cpu_profile is not None:
        cpu_profile_dir = cpu_profile
    else:
        cpu_profile_dir = f"{output}/cpu_profile"
        subprocess.run(f"spark_rapids_user_tools onprem profiling --csv {jar_arg} --local_folder {cpu_profile_dir} --eventlogs {cpu_log}", shell=True)

    ### run speedup factor generation
    subprocess.run(f"python generate_speedup_factors.py --cpu {cpu_profile_dir}/*/rapids_4_spark_profile --gpu {gpu_profile_dir}/*/rapids_4_spark_profile --output {output}/generatedScores.csv", shell=True)

# Run qualification

### set speedup factors to input or generated
speedups_arg = ""
if speedups is not None:
    speedups_arg = f"--speedup-factor-file {speedups}"
else:
    speedups_arg = f"--speedup-factor-file {output}/generatedScores.csv"

### run CPU qualification
cpu_tmp_dir = f"{output}/cpu"
subprocess.run(f"spark_rapids_user_tools onprem qualification {speedups_arg} {jar_arg} --local_folder {cpu_tmp_dir} --eventlogs {cpu_log}", shell=True)

# Parse and validate results

### CPU log parsing
cpu_app_info = pd.read_csv(glob.glob(f"{cpu_tmp_dir}/*/rapids_4_spark_qualification_output/rapids_4_spark_qualification_output.csv")[0])
cpu_query_info = cpu_app_info[["App Name", "App Duration", "Estimated GPU Duration", "Estimated GPU Speedup"]]

### GPU log parsing
gpu_query_info = pd.DataFrame(columns = ['App Name', 'GPU Duration'])
counter = 0

for app in glob.glob(f"{gpu_profile_dir}/*/rapids_4_spark_profile/*/application_information.csv"):
    app_info = pd.read_csv(app)
    new_row = pd.DataFrame({'App Name': app_info.loc[0]["appName"], 'GPU Duration': app_info.loc[0]["duration"]}, index=[counter])
    gpu_query_info = pd.concat([gpu_query_info, new_row])
    counter = counter+1

merged_info = cpu_query_info.merge(gpu_query_info, left_on='App Name', right_on='App Name')
merged_info["Duration Error (sec)"] = (merged_info["Estimated GPU Duration"] - merged_info["GPU Duration"])/1000.0
merged_info["Duration Error (pct)"] = (100.0*(merged_info["Estimated GPU Duration"] - merged_info["GPU Duration"])/merged_info["Estimated GPU Duration"]).apply(lambda x: round(x,2))
merged_info["GPU Speedup"] = (merged_info["App Duration"]/merged_info["GPU Duration"]).apply(lambda x: round(x,2))
merged_info["Speedup Error (abs)"] = merged_info["Estimated GPU Speedup"] - merged_info["GPU Speedup"]
merged_info["Speedup Error (pct)"] = (100.0*(merged_info["Estimated GPU Speedup"] - merged_info["GPU Speedup"])/merged_info["Estimated GPU Speedup"]).apply(lambda x: round(x,2))

print("==================================================")
print("              Application Details")
print("==================================================")
print(tabulate(merged_info, headers='keys', tablefmt='psql'))

print("==================================================")
print("            Duration Error Metrics ")
print("==================================================")
print("Average duration error (seconds)  = " + str(round(merged_info["Duration Error (sec)"].mean(),2)))
print("Median duration error (seconds)   = " + str(round(merged_info["Duration Error (sec)"].median(),2)))
print("Min duration error (seconds)      = " + str(round(merged_info["Duration Error (sec)"].min(),2)))
print("Max duration error (seconds)      = " + str(round(merged_info["Duration Error (sec)"].max(),2)))
print("Average duration error (diff pct) = " + str(round(merged_info["Duration Error (pct)"].mean(),2)))
print("Median duration error (diff pct)  = " + str(round(merged_info["Duration Error (pct)"].median(),2)))
print("Max duration error (diff pct)     = " + str(round(merged_info["Duration Error (pct)"].max(),2)))
print("Average duration error (diff sec) = " + str(round(merged_info["Duration Error (sec)"].abs().mean(),2)))
print("Median duration error (diff sec)  = " + str(round(merged_info["Duration Error (sec)"].abs().median(),2)))
print("Max duration error (diff sec)     = " + str(round(merged_info["Duration Error (sec)"].abs().max(),2)))
print("Average duration error (abs pct)  = " + str(round(merged_info["Duration Error (pct)"].abs().mean(),2)))
print("Median duration error (abs pct)   = " + str(round(merged_info["Duration Error (pct)"].abs().median(),2)))
print("Max duration error (abs pct)      = " + str(round(merged_info["Duration Error (pct)"].abs().max(),2)))
print("==================================================")
print("            Speedup Error Metrics ")
print("==================================================")
print("Average speedup error (diff)      = " + str(round(merged_info["Speedup Error (abs)"].mean(),2)))
print("Median speedup error (diff)       = " + str(round(merged_info["Speedup Error (abs)"].median(),2)))
print("Min speedup error (diff)          = " + str(round(merged_info["Speedup Error (abs)"].min(),2)))
print("Max speedup error (diff)          = " + str(round(merged_info["Speedup Error (abs)"].max(),2)))
print("Average speedup error (diff pct)  = " + str(round(merged_info["Speedup Error (pct)"].mean(),2)))
print("Median speedup error (diff pct    = " + str(round(merged_info["Speedup Error (pct)"].median(),2)))
print("Max speedup error (diff pct)      = " + str(round(merged_info["Speedup Error (pct)"].max(),2)))
print("Average speedup error (abs diff)  = " + str(round(merged_info["Speedup Error (abs)"].abs().mean(),2)))
print("Median speedup error (abs diff)   = " + str(round(merged_info["Speedup Error (abs)"].abs().median(),2)))
print("Max speedup error (abs diff)      = " + str(round(merged_info["Speedup Error (abs)"].abs().max(),2)))
print("Average speedup error (abs pct)   = " + str(round(merged_info["Speedup Error (pct)"].abs().mean(),2)))
print("Median speedup error (abs pct)    = " + str(round(merged_info["Speedup Error (pct)"].abs().median(),2)))
print("Max speedup error (abs pct)         = " + str(round(merged_info["Speedup Error (pct)"].abs().max(),2)))
