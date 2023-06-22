# RAPIDS Accelerator for Apache Spark Tools

This repo provides the tools to effectively use the [RAPIDS Accelerator for Apache Spark](https://github.com/NVIDIA/spark-rapids).

## Catalog

- [RAPIDS core tools](/core): Tools that help developers getting the most out of their Apache Spark applications
  without any code change:
  - Report acceleration potential of RAPIDS Accelerator for Apache Spark on a set of Spark applications.
  - Generate comprehensive profiling analysis for Apache Sparks executing on accelerated GPU instances. This information
    can be used to further tune and optimize the application.
- [spark-rapids-user-tools](/user_tools): A simple wrapper process around cloud service provider tools to run
  [RAPIDS core tools](/core) across multiple cloud platforms. In addition, the output educates the users on
  the cost savings and acceleration potential of the RAPIDS Accelerator for Apache Spark and makes recommendations to tune
  the application performance based on the cluster shape.
