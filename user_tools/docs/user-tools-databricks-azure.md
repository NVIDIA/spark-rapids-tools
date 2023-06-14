# RAPIDS User Tools on Databricks Azure

This is a guide for the RAPIDS tools for Apache Spark on [Databricks Azure](https://www.databricks.com/product/azure). At the end of this guide, the user will be able to run the RAPIDS tools to analyze the clusters and the applications running on Databricks Azure.

## Assumptions

The tool currently only supports event logs stored on ABFS ([Azure Blob File System](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri)). The remote output storage is also expected to be ABFS.

## Prerequisites

### 1.Databricks CLI

- Install the Databricks CLI. Follow the instructions on [Install the CLI](https://docs.databricks.com/dev-tools/cli/index.html#install-the-cli).
- Set the configuration settings and credentials of the Databricks CLI:
  - Set up authentication using a Databricks personal access token by following these [instructions](https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication-using-a-databricks-personal-access-token)
  - Test the authentication setup by following these [instructions](https://docs.databricks.com/dev-tools/cli/index.html#test-your-authentication-setup)
  - Verify that the access credentials are stored in the file `~/.databrickscfg` on Unix, Linux, or macOS, or in another file defined by 
    environment variable `DATABRICKS_CONFIG_FILE`.

### 2.Azure CLI

- TODO

### 3.RAPIDS tools

- Spark event logs:
  - The RAPIDS tools can process Apache Spark CPU event logs from Spark 2.0 or higher (raw, .lz4, .lzf, .snappy, .zstd)
  - For `qualification` commands, the event logs need to be archived to an accessible local or ABFS folder.

### 4.Install the package

- Install `spark-rapids-user-tools` with python [3.8, 3.10] using:
  - pip:  `pip install spark-rapids-user-tools`
  - wheel-file: `pip install <wheel-file>`
  - from source: `pip install -e .`
- verify the command is installed correctly by running
  ```bash
    spark_rapids_user_tools databricks_azure --help
  ```

