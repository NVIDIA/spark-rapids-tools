# Qualification and Profiling tools

The Qualification tool is used to look at a set of applications to determine if the RAPIDS Accelerator for Apache Spark
might be a good fit for those applications.

The Profiling tool generates information which can be used for debugging and profiling applications.
Information such as Spark version, executor information, properties and so on. This runs on either CPU or
GPU generated event logs.

Please refer to [Qualification tool documentation](https://docs.nvidia.com/spark-rapids/user-guide/latest/qualification/overview.html)
and [Profiling tool documentation](https://docs.nvidia.com/spark-rapids/user-guide/latest/profiling/overview.html)
for more details on how to use the tools.

## Build

We use [Maven](https://maven.apache.org) for the build. Simply run as below command:

```shell script
mvn clean package
```

After a successful build, the jar of 'rapids-4-spark-tools_2.12-*-SNAPSHOT.jar' will be in 'target/' directory.  
This will build the plugin for a single version of Spark. By default, this is Apache Spark 3.5.0.

For development purpose, you may need to run the tests against different spark versions.
To run the tests against a specific Spark version, you can use the `-Dbuildver=XXX` command line option.  
For instance to build Spark 3.5.1 you would use:

```shell script
mvn -Dbuildver=351 clean package
```

Run `mvn help:all-profiles` to list supported Spark versions.

### Setting up an Integrated Development Environment

Before proceeding with importing spark-rapids-tools into IDEA or switching to a different Spark release
profile, execute the install phase with the corresponding `buildver`, e.g. for Spark 3.5.0:

##### Manual Maven Install for a target Spark build

```bash
 mvn clean install -Dbuildver=350 -Dmaven.scaladoc.skip -DskipTests
```

##### Importing the project

To start working with the project in IDEA is as easy as importing the project as a Maven project.
Select the profile used in the mvn command above, e.g. `spark350` for Spark 3.5.0.

The tools project follows the same coding style guidelines as the Apache Spark
project.  For IntelliJ IDEA users, an example `idea-code-style-settings.xml` is available in the
`scripts` subdirectory of the root project folder.
