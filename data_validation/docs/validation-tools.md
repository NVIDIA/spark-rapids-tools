# Validation tools on Dataproc

This is a guide for the Validation tools for Apache Spark on [Google Cloud Dataproc](https://cloud.google.com/dataproc).  
At the end of this guide, the user will be able to run the validation tools to analyze
whether the Spark job using RAPIDS Accelerator(aka GPU Spark job) returns the same result as the CPU Spark job.

## Prerequisites

### 1.gcloud CLI

- Install the gcloud CLI. Follow the instructions on [gcloud-sdk-install](https://cloud.google.com/sdk/docs/install)
- Set the configuration settings and credentials of the gcloud CLI:
  - Initialize the gcloud CLI by following [these instructions](https://cloud.google.com/sdk/docs/initializing#initialize_the)
  - Grant authorization to the gcloud CLI [with a user account](https://cloud.google.com/sdk/docs/authorizing#authorize_with_a_user_account)
  - Manage gcloud CLI configurations. For more details, visit [gcloud-sdk-configurations](https://cloud.google.com/sdk/docs/configurations)
  - Verify that the following [gcloud CLI properties](https://cloud.google.com/sdk/docs/properties) are properly defined:
    - `dataproc/region`,
    - `compute/zone`,
    - `compute/region`
    - `core/project`
    
### 2.Install the package

- Install `spark_rapids_validation_tool` with python [3.8, 3.10] using:
   ```bash
    git clone https://github.com/NVIDIA/spark-rapids-tools.git
    cd spark-rapids-tools/data_validation 
    python -m build --wheel
    pip uninstall spark_rapids_validation_tool-23.2.0-py3-none-any.whl -y
    pip install dist/spark_rapids_validation_tool-23.2.0-py3-none-any.whl
  ```
- verify the command is installed correctly by running
  ```bash
    spark_rapids_validation_tool validation --help
  ```

## Use case scenario
The validation tool has two type of command, one is for metadata validation, the other is for data validation.
The goal for metadata validation is to compare metadata of the columns of 2 tables quickly 
to see if there is any huge difference, the metadata info including table row count, column count, the min/max/avg
values for some specific columns.
The goal for dataset validation is to compare the table datasets column by column, and show any difference in 
the final result. The output includes the PK(s) only in table1, the PK(s) only in table2 and a
result table with the same PK(s) but different values for that column(s).

## Validation command

Below are some demo cases about data validation command, assume there are already two hive table `datavalid3` and `datavalid4` in dataproc 
cluster `data-validation-test2`:

### Metadata Validation

#### Verify datavliad3 and datavalid4 metadata info with default configs:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 \
--format=hive --table1=datavalid3 --table2=datavalid4   valid_metadata
```
The sample output:
```

|--Top Level Metadata Info--|
+----------+--------+-----------+
| TableName|RowCount|ColumnCount|
+----------+--------+-----------+
|datavalid3|      11|          9|
|datavalid4|      11|          9|
+----------+--------+-----------+

None
|--Metadata Diff Info--|
+----------+-----+-----+-----+-----+-------+-------+--------+--------+-----------+-----------+
|ColumnName|min_A|min_B|max_A|max_B|  avg_A|  avg_B|stddev_A|stddev_B|countdist_A|countdist_B|
+----------+-----+-----+-----+-----+-------+-------+--------+--------+-----------+-----------+
|      col1|     |     | 12.0| 11.0| 6.0909|    6.0|  3.4772|  3.3166|           |           |
|      col3|     |     |     |     | 5.0909| 5.1818|  3.1766|    3.06|           |           |
|      col4|     |     |     |     |       |       |        |        |          4|          5|
|      col6|     |     |     |     |       |       |        |        |         10|         11|
|      col7|     |     |     |     |       |       |        |        |         10|         11|
|      col8|12.34|12.33|     |     |12.4345|12.4336|  0.3003|  0.3006|          3|          4|
+----------+-----+-----+-----+-----+-------+-------+--------+--------+-----------+-----------+

```

#### Verify datavaliad3 and datavalid4 metadata info with specific columns:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4  -include_column=col2,col3,col4 -exclude_column=col3 valid_metadata
```
The sample output:
```
|--Top Level Metadata Info--|
+----------+--------+-----------+
| TableName|RowCount|ColumnCount|
+----------+--------+-----------+
|datavalid3|      11|          9|
|datavalid4|      11|          9|
+----------+--------+-----------+

None
|--Metadata Diff Info--|
+----------+-----+-----+-----+-----+-----+-----+--------+--------+-----------+-----------+
|ColumnName|min_A|min_B|max_A|max_B|avg_A|avg_B|stddev_A|stddev_B|countdist_A|countdist_B|
+----------+-----+-----+-----+-----+-----+-----+--------+--------+-----------+-----------+
|      col4|     |     |     |     |     |     |        |        |          4|          5|
+----------+-----+-----+-----+-----+-----+-----+--------+--------+-----------+-----------+

```

#### Verify table with specific partition and filter conditions:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4 --table1_partition="col9=20230310" --table2_partition="col9=20230310" --filter="col4='bar'" valid_metadata
```
The sample output:
```
|--Top Level Metadata Info--|
+----------+--------+-----------+
| TableName|RowCount|ColumnCount|
+----------+--------+-----------+
|datavalid3|       4|          9|
|datavalid4|       3|          9|
+----------+--------+-----------+

None
|--Metadata Diff Info--|
+----------+-----+------+-----+-----+-------+-------+--------+--------+-----------+-----------+
|ColumnName|min_A| min_B|max_A|max_B|  avg_A|  avg_B|stddev_A|stddev_B|countdist_A|countdist_B|
+----------+-----+------+-----+-----+-------+-------+--------+--------+-----------+-----------+
|      col1|     |      | 12.0| 11.0|    7.5| 6.3333|  4.4347|  4.5092|          4|          3|
|      col3|     |      |     |     |    3.0| 3.6667|  2.8284|  3.0551|           |           |
|      col8|12.34|12.345|     |     |12.5925|12.6767|  0.4983|  0.5745|          3|          2|
+----------+-----+------+-----+-----+-------+-------+--------+--------+-----------+-----------+

```

#### In a case that compare tables but some does not exist:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid9  -include_column=col2,col3,col4 -exclude_column=col3 valid_metadata
```
The sample output:
```
|--Table datavalid9 does not exist!--|
|--Please Check The Inputs --|
```

#### In a case that some data type not supported in metadata validation:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4  --include_column=col2,col3,col5 --exclude_column=col3 valid_metadata
```
The sample output:
```
|--Unsupported metadata included data type: date for column: StructField(col5,DateType,true)--|
|--Please Check The Inputs --|
```

### Dataset Validation

#### Print all different data for all columns and all data type:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4 --pk=col1   valid_data
```
The sample output:
```
+----+
|col1|
+----+
|  12|
+----+

None
|--PK(s) only in datavalid4 :--|
+----+
|col1|
+----+
|  11|
+----+

None
|--Columns with same PK(s) but diff values :--|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------------------+--------------------+--------------------+--------------------+
|col1|t1_col5|t2_col5|t1_col8|t2_col8|t1_col4|t2_col4|t1_col2|t2_col2|t1_col3|t2_col3|t1_col9|t2_col9|             t1_col6|             t2_col6|             t1_col7|             t2_col7|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------------------+--------------------+--------------------+--------------------+
|  10|       |       | 12.340| 12.330|    bar|    baa|       |       |      1|      2|       |       |{'key19': 'value1...|{'key19': 'value1...|{'key19': ['value...|{'key19': ['value...|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------------------+--------------------+--------------------+--------------------+
```

#### Verify two tables dataset with some partition condition:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4 --pk=col1  --table1_partition=col9='20230310'  valid_data
```
The sample output:
```
+----+
|col1|
+----+
|  12|
+----+

None
|--PK(s) only in datavalid4 :--|
+----+
|col1|
+----+
|  11|
+----+

None
|--Columns with same PK(s) but diff values :--|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------------------+--------------------+--------------------+--------------------+
|col1|t1_col5|t2_col5|t1_col8|t2_col8|t1_col4|t2_col4|t1_col2|t2_col2|t1_col3|t2_col3|t1_col9|t2_col9|             t1_col6|             t2_col6|             t1_col7|             t2_col7|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------------------+--------------------+--------------------+--------------------+
|  10|       |       | 12.340| 12.330|    bar|    baa|       |       |      1|      2|       |       |{'key19': 'value1...|{'key19': 'value1...|{'key19': ['value...|{'key19': ['value...|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+--------------------+--------------------+--------------------+--------------------+
```

#### Verify two tables dataset with filter:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4 --pk=col1  --table1_partition=col9='20230310'  -filter="col4='bar'" valid_data
```
The sample output:
```
+----+
|col1|
+----+
|  12|
|  10|
+----+

None
|--PK(s) only in datavalid4 :--|
+----+
|col1|
+----+
|  11|
+----+

None
|--Columns with same PK(s) but diff values :--|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
|col1|t1_col5|t2_col5|t1_col8|t2_col8|t1_col4|t2_col4|t1_col2|t2_col2|t1_col3|t2_col3|t1_col9|t2_col9|t1_col6|t2_col6|t1_col7|t2_col7|
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
+----+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-------+
```

#### Comparing datasets and save the output to GCS:
Validation command:
```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 --format=hive \
--table1=datavalid3 --table2=datavalid4 --pk=col1  --table1_partition=col9='20230310' 
--table2_partition=col9='20230310' 
--output_path=gs://XXX/output --output_format=parquet valid_data
```
There will be a folder in GCS saving the diff dataset.