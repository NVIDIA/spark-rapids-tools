# QualX: Spark RAPIDS Qualification Tool using XGBoost

Tool to qualify Spark applications for GPU acceleration, based on XGBoost.

## Usage

### Prediction

To predict the speedup of running a Spark application with Spark RAPIDS on GPUs, use the cmd below. It uses an XGBoost model trained on matching CPU and GPU runs of various Spark applications.

```bash
spark_rapids prediction --qual_output </path/to/qual_output> --prof_output </path/to/prof_output> --output_folder </path/to/save/csv/files>
```

#### Arguments:
- qual_output: Path to a directory containing qualification tool output.
- prof_output: Path to a directory containing profiling tool output.
- output_folder: Path to store the output.

### Training

To train an XGBoost model on the specific dataset, follow these steps below. Refer to [Getting Started](../README.md#getting-started) section for installing the required dependencies for training.

Set the following environment variables:
```bash
export SPARK_HOME=/path/to/spark
export SPARK_RAPIDS_TOOL_JAR=/path/to/rapids-4-spark-tools-0.1.0-SNAPSHOT.jar
export QUALX_DATA_DIR=/path/to/qualx/datasets
export QUALX_CACHE_DIR=/path/to/qualx/cache
```

Run the following command to train the model:
```bash
spark_rapids train --dataset </path/to/dataset/files(s)> --model </path/to/save/trained/model> --output_folder </path/to/save/csv/files> --n_trials <number_of_trials>
```

#### Arguments:
- dataset: Path to a folder containing one or more dataset JSON files.
- model: Path to save the trained XGBoost model.
- output_folder: Path to store the output.
- n_trials: Number of trials for hyperparameter search.

## Notes
- `QUALX_DATA_DIR` should already exist and must contain the dataset files (e.g., from SwiftStack).
- `QUALX_CACHE_DIR` will be created automatically if it does not already exist and will store intermediate files generated during processing (e.g., profiling output).