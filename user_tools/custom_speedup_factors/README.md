# Custom Speedup Factors for Workload Qualification

## Purpose

Speedup factor estimation for the qualification tool is used for determining the estimated runtime for an application on GPU using Spark RAPIDS.  The speedup factors generate multipliers at an exec (and potentially expression) level that are used in the qualification tool for GPU estimates.

## Prerequisites

1. python >= 3.8
2. Necessary Python dependencies: `pandas`, `argparse`, `spark-rapids-user-tools`

## Generating Custom Speedup Factors

The high-level process to generate speedup factors for an environment is as follows:

1. Event log generation
   1. Run the NDS SF3K benchmark on CPU cluster along with any other representative jobs and save event log(s).  Follow steps documented in the [NDS README](https://github.com/NVIDIA/spark-rapids-benchmarks/blob/dev/nds/README.md) for running the Power Run.
   2. Run the NDS SF3K benchmark on GPU cluster along with any other representative jobs and save event log(s).  Follow steps documented in the [NDS README](https://github.com/NVIDIA/spark-rapids-benchmarks/blob/dev/nds/README.md) for running the Power Run.
2. Job profiler analysis
   1. Run the Spark RAPIDS profiling tool against the CPU and GPU event log to get stage-level duration metrics.
```
spark_rapids_user_tools onprem profiling --csv --eventlogs CPU-3k --local_folder CPU-3k-profile

spark_rapids_user_tools onprem profiling --csv --eventlogs GPU-3k --local_folder GPU-3k-profile
```
3. Speedup factor generation
   1. Run the speedup factor generation script, passing the CPU and GPU profiler output along with a CSV output filename.
```
python generate_speedup_factors.py --cpu CPU-3k-profile/rapids_4_spark_profile --gpu GPU-3k-profile/rapids_4_spark_profile --output newScores.csv
```

The script will generate the new scores in the output specified by the `--output` argument.

## Running Workload Qualification with Custom Speedup Factors

Now that you have a custom *operatorsScore.csv* file, you can run the Spark RAPIDS qualification tool using it to get estimations applicable for your environment.  Here is the command to run with a custom speedup factor file:
```
spark_rapids_user_tools onprem qualification --speedup-factor-file newScores.csv --eventlogs <CPU-event-logs>
```

## Validating Custom Speedup Factors

There is a utility script in the directory to allow for validation of custom speedup factors given CPU and GPU event logs for a corresponding job or set of jobs.  By default, the script will generate custom speedup factors, run the qualification tool with the generated custom speed up factors, and then generate validation metrics for the estimations against the actuals.

Example execution of the script:
```
python validate_qualification_estimates.py --cpu_log CPU-nds-eventlog --gpu_log GPU-nds-eventlog --output test-speedup
```

The script also allows you to pass in a custom speedup factor file if you have previously generated them.  Example:
```
python validate_qualification_estimates.py --cpu_log CPU-nds-eventlog --gpu_log GPU-nds-eventlog --output test-speedup --speedups test-scores.csv
```

Other options include passing in the CPU and/or GPU profiler output if that has already been done via the `cpu_profile` and `gpu_profile` arguments.  Additionally, you can pass in a custom tools jar via `--jar` if that is needed.
