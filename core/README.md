# Qualification and Profiling tools

The Qualification tool is used to look at a set of applications to determine if the RAPIDS Accelerator for Apache Spark
might be a good fit for those applications.

The Profiling tool generates information which can be used for debugging and profiling applications.
Information such as Spark version, executor information, properties and so on. This runs on either CPU or
GPU generated event logs.

Please refer to [Qualification tool documentation](docs/spark-qualification-tool.md) 
and [Profiling tool documentation](docs/spark-profiling-tool.md)
for more details on how to use the tools.

## Build

We use [Maven](https://maven.apache.org) for the build. Simply run as below command:

```shell script
mvn clean package
```

After a successful build, the jar of 'rapids-4-spark-tools_2.12-*-SNAPSHOT.jar' will be in 'target/' directory.  
This will build the plugin for a single version of Spark. By default, this is Apache Spark 3.3.3.

For development purpose, you may need to run the tests against different spark versions.
To run the tests against a specific Spark version, you can use the `-Dbuildver=XXX` command line option.  
For instance to build Spark 3.4.1 you would use:

```shell script
mvn -Dbuildver=341 clean package
```

Run `mvn help:all-profiles` to list supported Spark versions.
