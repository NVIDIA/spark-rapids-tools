# Qualification and Profiling tools core

The Qualification tool is used to look at a set of applications to determine if the RAPIDS Accelerator for Apache Spark
might be a good fit for those applications.

The Profiling tool gathers and generates information that can be used for debugging and profiling applications (i.e. Spark version, executor properties, environment information, etc.).
This profiling can be ran on either CPU or GPU generated event logs.

Please refer to [Qualification tool documentation](docs/spark-qualification-tool.md) 
and [Profiling tool documentation](docs/spark-profiling-tool.md)
for details on how to use the respective tools.

## Build

We use [Maven](https://maven.apache.org) for the core build. Simply run:

```shell script
mvn clean package
```

After a successful build, the `target/` directory will contain the application jar `rapids-4-spark-tools_2.12-${TOOL_VERSION}-SNAPSHOT.jar`.
