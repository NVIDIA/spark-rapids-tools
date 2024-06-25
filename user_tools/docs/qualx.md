# QualX: Spark RAPIDS Qualification Tool using XGBoost

Tool to qualify Spark applications for GPU acceleration, based on XGBoost.

## Usage

### Prediction

To predict the speedup of running a Spark application with Spark RAPIDS on GPUs, use the cmd below. It uses an XGBoost model trained on matching CPU and GPU runs of various Spark applications.

```bash
spark_rapids prediction \
--qual_output </path/to/qual_output> \
--prof_output </path/to/prof_output> \
--output_folder </path/to/save/csv/files>
```

Arguments:
- qual_output: Path to a directory containing qualification tool output.
- prof_output: Path to a directory containing profiling tool output.
- output_folder: Path to store the output.

### Training

To train an XGBoost model on a specific dataset, follow these steps below. Refer to [Getting Started](../README.md#getting-started) section for installing the required dependencies for training.

#### Environment Setup

Training requires the following environment variables to be set:
```bash
export SPARK_HOME=/path/to/spark
export SPARK_RAPIDS_TOOLS_JAR=/path/to/rapids-4-spark-tools-0.1.0-SNAPSHOT.jar
export QUALX_DATA_DIR=/path/to/qualx/datasets
export QUALX_CACHE_DIR=/path/to/qualx/cache
```

Notes:
- `QUALX_DATA_DIR` should be a valid local path containing the training data.
- `QUALX_CACHE_DIR` stores intermediate files generated during processing (e.g., profiling output). It will be created automatically if it does not exist.  The directory can be deleted to invalidate the cache.

#### Data Collection

Training expects the training data (eventlogs) to be collected in the location specified by `QUALX_DATA_DIR`.  This directory MUST be structured as follows:
```
${QUALX_DATA_DIR}/<platform>/<dataset>/<app_name>/eventlogs/{cpu,gpu}/*
```

Where:
- platform: one of the supported platforms, e.g. `databricks-aws`, `databricks-azure`, `dataproc`, `emr`, or `onprem`.
- dataset: unique name for a group of related Spark applications, e.g. `nds`.
- app_name: unique name for a specific run of a Spark application, e.g. `powerrun`.  Note that this can vary from the actual Spark application name, as long as it uniquely identifies this dataset.

For example, given eventlogs for an on-prem NDS powerrun, we might see something like this:
```
$QUALX_DATA_DIR
└── onprem
    └── nds
       └── powerrun
           └── eventlogs
               ├── cpu
               │   └── app-20240412202337-0001
               └── gpu
                   └── app-20240412202939-0000
```

#### Dataset Metadata

Once the eventlogs have been collected to the `QUALX_DATA_DIR`, we must define some metadata about the dataset in JSON format.

For simplicitly, we will continue the example from above:
```bash
cat <<EOF > datasets/onprem/nds.json
{
    "nds_powerrun": {
        "eventlogs": [
            "${QUALX_DATA_DIR}/onprem/nds/powerrun/eventlogs/cpu",
            "${QUALX_DATA_DIR}/onprem/nds/powerrun/eventlogs/gpu"
        ],
        "app_meta": {
            "app-20240412202337-0001": {"runType": "CPU", "scaleFactor": 1},
            "app-20240412202939-0000": {"runType": "GPU", "scaleFactor": 1},
        }
    }
}
EOF
```

Notes:
- The top level key is a unique name for this set of eventlogs.  Generally, this is the combination of the dataset and app_name by convention.
- The "eventlogs" key is required and should point to the location of the eventlogs on disk.
- The "app_meta" key defines application-specific metadata, such as "runType" and "scaleFactor".
  - The "scaleFactor" key allows multiple eventlogs from the same application run at different scales.

These JSON metadata files should be collected into a single directory where the first level must be a supported platform, e.g.
```
datasets
└── onprem
    └── nds.json
```

#### Command

After the datasets have been collected and the metadata defined, run training as follows:
```bash
spark_rapids train \
--dataset </path/to/dataset/files(s)> \
--model </path/to/save/trained/model> \
--output_folder </path/to/save/csv/files> \
--n_trials <number_of_trials>
```

Arguments:
- dataset: Path to a folder containing one or more dataset JSON files.
- model: Path to save the trained XGBoost model.
- output_folder: Path to store the output.
- n_trials: Number of trials for hyperparameter search, default: 200.

Continuing the example from above, we would use:
```bash
spark_rapids train \
--dataset datasets \
--model onprem.json \
--output_folder train_output
```

Once satisfied with the model, we can just overwrite an existing pre-trained model to use it as a drop-in replacement:
```bash
cp onprem.json user_tools/src/spark_rapids_pytools/resources/qualx/models/xgboost
```

