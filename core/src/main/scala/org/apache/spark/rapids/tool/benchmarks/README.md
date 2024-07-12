# Benchmarking Tools

This package contains the relevant classes to write and run benchmarks for `RAPIDS TOOLS` for Apache Spark.

## Writing a benchmark -
* Extend `BenchmarkBase` and override the `runBenchmarkSuite` function
* Write logically similar benchmarks inside the `runBenchmark` which will add a header to the output with the name provided.
* Now for each case to be tested, use the Benchmark class `addCase` to write various cases of the same Benchmark.
* Call the run function to run the benchmark
* Refer included example benchmark - [`SingleThreadedQualToolBenchmark`](./SingleThreadedQualToolBenchmark.scala) for implementation details

## Running the benchmark -
Use the java command to run the created benchmark class with the following supported params -
* `-i` : total number of iterations to run to calculate average metrics
* `-w` : total number of warmup iterations to run before calculating the final metrics ( warmup is relevant so that final results are not skewed by the initial java classloading times )
* `-o` : output directory where to store the final result file. Defaults to the directory rapids-tools-benchmark in the root directory
* `-f` : output format of the stored result. Currently supports text. Json to be added in future iterations
* `-a` : input arguments to pass the underlying benchmark classes

#### Running the Benchmark class directly
```shell
java -cp $CLASSPATH \
org.apache.spark.rapids.tool.benchmarks.SingleThreadedQualToolBenchmark \
-i 3 -w 3 -a " --output-directory output eventlogs"
```
`CLASSPATH` should be the path relative to which the Benchmarking class being passed. 
Below examples, classpath contains the tool jar relative to which the Benchmarking class is passed
with the package name ( org.apache.spark.rapids.tool.benchmarks.SingleThreadedQualToolBenchmark ).

#### Running the Benchmark class using tools jar
```shell
java -cp $RAPIDS_TOOLS_JAR:$SPARK_HOME/jars/* \
org.apache.spark.rapids.tool.benchmarks.SingleThreadedQualToolBenchmark \
-i 3 -w 3 -a " $EVENT_LOGS_DIR"
```
* `$RAPIDS_TOOLS_JAR` : Path to the rapids tools jar
* `$SPARK_HOME/jars/*` : Include the `SPARK` jars in the classpath in case benchmarking `QUALIFICATION/PROFILING` tool 
* `$EVENT_LOGS_DIR` : Path to the event logs directory