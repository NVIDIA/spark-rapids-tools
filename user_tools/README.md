# spark-rapids-user-tools

User tools to help with the adoption, installation, execution, and tuning of RAPIDS Accelerator for Apache Spark.

The wrapper improves end-user experience within the following dimensions:
1. **Qualification**: Educate the CPU customer on the cost savings and acceleration potential of RAPIDS Accelerator for
   Apache Spark. The output shows a list of apps recommended for RAPIDS Accelerator for Apache Spark with estimated savings
   and speed-up.
2. **Bootstrap**: Provide optimized RAPIDS Accelerator for Apache Spark configs based on GPU cluster shape. The output
   shows updated Spark config settings on driver node.
3. **Tuning**: Tune RAPIDS Accelerator for Apache Spark configs based on initial job run leveraging Spark event logs. The output
   shows recommended per-app RAPIDS Accelerator for Apache Spark config settings.
4. **Diagnostics**: Run diagnostic functions to validate the Dataproc with RAPIDS Accelerator for Apache Spark environment to
   make sure the cluster is healthy and ready for Spark jobs.


## Getting started

Set up a Python environment with a version between 3.8 and 3.10

1. Run the project in a virtual environment.
    ```sh
    $ python -m venv .venv
    $ source .venv/bin/activate
    ```
2. Install spark-rapids-user-tools 
    - Using released package.
      
      ```sh
      $ pip install spark-rapids-user-tools
      ```
    - Using local version from the repo

      ```sh
      $ pip install -e .
      ```
    - Using wheel package built from the repo

      ```sh
      $ pip install build
      $ python -m build --wheel
      $ pip install <wheel-file>
      ```
3. Make sure to install CSP SDK if you plan to run the tool wrapper.

## Usage and supported platforms

Please refer to [spark-rapids-user-tools guide](https://github.com/NVIDIA/spark-rapids-tools/blob/main/user_tools/docs/index.md) for details on how to use the tools
and the platform.

## What's new

Please refer to [CHANGELOG.md](https://github.com/NVIDIA/spark-rapids-tools/blob/main/CHANGELOG.md) for our latest changes.
