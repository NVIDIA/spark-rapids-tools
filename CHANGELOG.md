
<br/>

## Release [v24.02.2](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.02.2)
Generated on 2024-03-27
### User Tools

- Override estimated speedups when estimation model is enabled ([#885](https://github.com/NVIDIA/spark-rapids-tools/pull/885))
- [FEA] Make top candidates view as the default view in user-tools ([#879](https://github.com/NVIDIA/spark-rapids-tools/pull/879))
- Introduce new csv file containing output for all apps before grouping ([#875](https://github.com/NVIDIA/spark-rapids-tools/pull/875))
- Fix calculation of unsupported operators stages duration and update output row ([#874](https://github.com/NVIDIA/spark-rapids-tools/pull/874))
- Implement top candidate filter for user tools CLI output ([#866](https://github.com/NVIDIA/spark-rapids-tools/pull/866))

### Core

- [FEA] Skip Databricks Photon jobs at app level in Qualification tool ([#886](https://github.com/NVIDIA/spark-rapids-tools/pull/886))
- [FEA] Add Estimation Model to Qualification CLI ([#870](https://github.com/NVIDIA/spark-rapids-tools/pull/870))
- Add rootExecutionID to output csv files ([#871](https://github.com/NVIDIA/spark-rapids-tools/pull/871))
- [FEA] Generate updated supported CSV files from plugin repo ([#847](https://github.com/NVIDIA/spark-rapids-tools/pull/847))
- Add action column to qual execs output ([#859](https://github.com/NVIDIA/spark-rapids-tools/pull/859))
- Extend supportLevels in PluginTypeChecker ([#863](https://github.com/NVIDIA/spark-rapids-tools/pull/863))
- Propagate Reason/Notes for operators disabled by default from plugin to Qualification tool unsupported operators csv file ([#850](https://github.com/NVIDIA/spark-rapids-tools/pull/850))

### Miscellaneous

- Bump default Spark-version to 3.5.0 ([#877](https://github.com/NVIDIA/spark-rapids-tools/pull/877))
- Update Github actions version ([#876](https://github.com/NVIDIA/spark-rapids-tools/pull/876))


<br/>

## Release [v24.02.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.02.1)
Generated on 2024-03-15
### User Tools

-  Remove redundant initialization scripts from user tools output ([#830](https://github.com/NVIDIA/spark-rapids-tools/pull/830))
- [DOC] Update Databricks Azure user tool setup instructions for output format ([#826](https://github.com/NVIDIA/spark-rapids-tools/pull/826))
- Estimate cluster instances and generate cost savings  ([#803](https://github.com/NVIDIA/spark-rapids-tools/pull/803))

### Core

- Fix implementation of processSQLPlanMetrics in Profiler ([#853](https://github.com/NVIDIA/spark-rapids-tools/pull/853))
- Deduplicate SQL duration wallclock time for databricks eventlog ([#810](https://github.com/NVIDIA/spark-rapids-tools/pull/810))
- Consider additional factors in spark.sql.shuffle.partitions recommendation in Autotuner ([#722](https://github.com/NVIDIA/spark-rapids-tools/pull/722))
- Fix case matching error In AutoTuner ([#828](https://github.com/NVIDIA/spark-rapids-tools/pull/828))
- Fix ReadSchema in Qualification tool and NPE in Profiling tool ([#825](https://github.com/NVIDIA/spark-rapids-tools/pull/825))
- AutoTuner does not process arguments skipList and limitedLogic ([#812](https://github.com/NVIDIA/spark-rapids-tools/pull/812))


<br/>

## Release [v24.02.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.02.0)
Generated on 2024-02-24
### User Tools

- Fix missing config file for Dataproc GKE ([#778](https://github.com/NVIDIA/spark-rapids-tools/pull/778))
- [FEA] Qualification user_tools runs AutoTuner by default ([#771](https://github.com/NVIDIA/spark-rapids-tools/pull/771))
- [BUG] Fix databricks-aws user profiling tool error with `--gpu_cluster` argument ([#707](https://github.com/NVIDIA/spark-rapids-tools/pull/707))

### Core

- [FEA] Qualification tool should mark WriteIntoDeltaCommand as supported ([#801](https://github.com/NVIDIA/spark-rapids-tools/pull/801))
- Qualification tool should mark SubqueryExec as IgnoreNoPerf ([#798](https://github.com/NVIDIA/spark-rapids-tools/pull/798))
- Generate cluster information from event logs in Qualification tool ([#789](https://github.com/NVIDIA/spark-rapids-tools/pull/789))
- Sync up supported ops for 24.02 plugin release ([#796](https://github.com/NVIDIA/spark-rapids-tools/pull/796))
- Qualification should mark empty2null as supported ([#791](https://github.com/NVIDIA/spark-rapids-tools/pull/791))
- Incorrect parsing of aggregates in DB queries ([#790](https://github.com/NVIDIA/spark-rapids-tools/pull/790))
- Qualification should mark WriteFiles as supported ([#784](https://github.com/NVIDIA/spark-rapids-tools/pull/784))
-  Introduce GpuDevice abstraction and refactor AutoTuner  ([#740](https://github.com/NVIDIA/spark-rapids-tools/pull/740))
- Consolidate unsupportedOperators into a single view ([#766](https://github.com/NVIDIA/spark-rapids-tools/pull/766))
- Speedup generator script fails after adding runtime_properties ([#776](https://github.com/NVIDIA/spark-rapids-tools/pull/776))
- Tools fail on DB10.4 clusters with IllegalArgException ([#768](https://github.com/NVIDIA/spark-rapids-tools/pull/768))
- Fix SparkPlanGraphCluster constructor for DB Platforms ([#765](https://github.com/NVIDIA/spark-rapids-tools/pull/765))
- Amendment to PR-763 ([#764](https://github.com/NVIDIA/spark-rapids-tools/pull/764))
- Fix SQLPLanMetric constructor for DB Platforms ([#763](https://github.com/NVIDIA/spark-rapids-tools/pull/763))
- Fix node constructor for DB platforms ([#761](https://github.com/NVIDIA/spark-rapids-tools/pull/761))
- Add penalty for stages with UDF's ([#757](https://github.com/NVIDIA/spark-rapids-tools/pull/757))
- Add support to appendDataExecV1 and overwriteByExprExecV1 ([#756](https://github.com/NVIDIA/spark-rapids-tools/pull/756))
- Qualification fails to detect sortMergeJoin with arguments ([#754](https://github.com/NVIDIA/spark-rapids-tools/pull/754))
- Fix Qualification crash during aggregation of stats ([#753](https://github.com/NVIDIA/spark-rapids-tools/pull/753))
- [FEA] Extend the list of operators to be ignored in Qualification ([#745](https://github.com/NVIDIA/spark-rapids-tools/pull/745))
- Remove ReusedSubquery from SparkPlanGraph construction ([#741](https://github.com/NVIDIA/spark-rapids-tools/pull/741))
- Update unsupported operator csv file's app duration column ([#748](https://github.com/NVIDIA/spark-rapids-tools/pull/748))
- [FEA] Qualification tool triggers the AutoTuner module ([#739](https://github.com/NVIDIA/spark-rapids-tools/pull/739))
- Disable support of GetJsonObject in Qualification tool ([#737](https://github.com/NVIDIA/spark-rapids-tools/pull/737))
- [FEA] AutoTuner warns that non-utf8 may not support some GPU expressions ([#736](https://github.com/NVIDIA/spark-rapids-tools/pull/736))
- [FEA] AutoTuner should not skip non-gpu eventlogs ([#728](https://github.com/NVIDIA/spark-rapids-tools/pull/728))

### Miscellaneous

- Add auto-copyright for precommits ([#732](https://github.com/NVIDIA/spark-rapids-tools/pull/732))


<br/>

## Release [v23.12.3](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.12.3)
Generated on 2024-01-12
### Core

- Add support of HiveTableScan and InsertIntoHive text-format ([#723](https://github.com/NVIDIA/spark-rapids-tools/pull/723))
- Fix compilation error with JDK11 ([#720](https://github.com/NVIDIA/spark-rapids-tools/pull/720))
- Generate an output file with runtime and build information ([#705](https://github.com/NVIDIA/spark-rapids-tools/pull/705))
- AutoTuner should poll maven-meta to retrieve the latest jar version  ([#711](https://github.com/NVIDIA/spark-rapids-tools/pull/711))
-  Profiling tool : Profiling tool throws NPE when appInfo is null and unchecked ([#640](https://github.com/NVIDIA/spark-rapids-tools/pull/640))
- Add support to parse_url host and protocol ([#708](https://github.com/NVIDIA/spark-rapids-tools/pull/708))
- [FEA] Profiling tool auto-tuner should consider `spark.databricks.adaptive.autoOptimizeShuffle.enabled` ([#710](https://github.com/NVIDIA/spark-rapids-tools/pull/710))
- [FEA] Profiler autotuner should only specify standard Spark versions for shuffle manager setting ([#662](https://github.com/NVIDIA/spark-rapids-tools/pull/662))

### Miscellaneous

- [FEA] Enable AQE related recommendations in Profiler Auto-tuner ([#688](https://github.com/NVIDIA/spark-rapids-tools/pull/688))


<br/>

## Release [v23.12.2](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.12.2)
Generated on 2023-12-27
### User Tools

- Polling maven-metadata.xml to pull the latest tools jar ([#703](https://github.com/NVIDIA/spark-rapids-tools/pull/703))

### Core

- Update pom to fail on warnings ([#701](https://github.com/NVIDIA/spark-rapids-tools/pull/701))


<br/>

## Release [v23.12.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.12.1)
Generated on 2023-12-23
- no changes
<br/>

## Release [v23.12.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.12.0)
Generated on 2023-12-20
### User Tools

- Fix user qualification tool runtime error in `get_platform_name` for onprem platform ([#684](https://github.com/NVIDIA/spark-rapids-tools/pull/684))
- [FEA] User tool should pass `--platform` option/argument to Profiling tool ([#679](https://github.com/NVIDIA/spark-rapids-tools/pull/679))
- Fix incorrect processing of short flags for user tools cli ([#677](https://github.com/NVIDIA/spark-rapids-tools/pull/677))
- Updating new CLI name from ascli to spark_rapids ([#673](https://github.com/NVIDIA/spark-rapids-tools/pull/673))
- Bump pyarrow version ([#664](https://github.com/NVIDIA/spark-rapids-tools/pull/664))
- Improve new CLI testing ensuring complete coverage of arguments cases ([#652](https://github.com/NVIDIA/spark-rapids-tools/pull/652))

### Core

- Qualification tool: Add more information for unsupported operators  ([#680](https://github.com/NVIDIA/spark-rapids-tools/pull/680))
- Sync Execs and Expressions from spark-rapids resources ([#691](https://github.com/NVIDIA/spark-rapids-tools/pull/691))
- Support parsing of inprogress eventlogs ([#686](https://github.com/NVIDIA/spark-rapids-tools/pull/686))
- Enable features via config that are off by default in the profiler AutoTuner ([#668](https://github.com/NVIDIA/spark-rapids-tools/pull/668))
- Fix platform names as string constants and reduce redundancy in unit tests ([#667](https://github.com/NVIDIA/spark-rapids-tools/pull/667))
- Unified platform handling and fetching of operator score files ([#661](https://github.com/NVIDIA/spark-rapids-tools/pull/661))
- Qualification tool: Ignore some of the unsupported Execs from output ([#665](https://github.com/NVIDIA/spark-rapids-tools/pull/665))

### Miscellaneous

- add markdown link checker ([#672](https://github.com/NVIDIA/spark-rapids-tools/pull/672))


<br/>

## Release [v23.10.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.10.1)
Generated on 2023-11-16
### User Tools

- Updating tools docs to remove dead links and profiling docs to not require cluster/worker info ([#651](https://github.com/NVIDIA/spark-rapids-tools/pull/651))
- Updating autotuner to generation recommendation always, even without cluster info ([#650](https://github.com/NVIDIA/spark-rapids-tools/pull/650))
- Updating dataproc container cost to be multiplied by number of cores ([#648](https://github.com/NVIDIA/spark-rapids-tools/pull/648))
- [BUG] Support autoscaling clusters for user qualification tool on Databricks platforms ([#647](https://github.com/NVIDIA/spark-rapids-tools/pull/647))
- Support extra arguments in new user tools CLI ([#646](https://github.com/NVIDIA/spark-rapids-tools/pull/646))
- Improve logs with user tools and jar version details ([#642](https://github.com/NVIDIA/spark-rapids-tools/pull/642))

### Core

- Profiling tool: Add support for driver log as input to generate unsupported operators report ([#654](https://github.com/NVIDIA/spark-rapids-tools/pull/654))
- Updating tools docs to remove dead links and profiling docs to not require cluster/worker info ([#651](https://github.com/NVIDIA/spark-rapids-tools/pull/651))
- Updating autotuner to generation recommendation always, even without cluster info ([#650](https://github.com/NVIDIA/spark-rapids-tools/pull/650))
- Qualification tool: Enhance mapping of Execs to stages ([#634](https://github.com/NVIDIA/spark-rapids-tools/pull/634))


<br/>

## Release [v23.10.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.10.0)
Generated on 2023-10-30
### User Tools

- Fix system command processing during logging in user tools ([#633](https://github.com/NVIDIA/spark-rapids-tools/pull/633))
- Fix spinner animation blocking user input in diagnostic tool ([#631](https://github.com/NVIDIA/spark-rapids-tools/pull/631))
- Enable Dynamic 'Zone' Configuration for Dataproc User Tools ([#629](https://github.com/NVIDIA/spark-rapids-tools/pull/629))

### Core

- Profiling tool : Update readSchema string parser ([#635](https://github.com/NVIDIA/spark-rapids-tools/pull/635))
- [FEA] Fix empty softwareProperties field in worker_info.yaml file for profiling tool ([#623](https://github.com/NVIDIA/spark-rapids-tools/pull/623))


<br/>

## Release [v23.08.2](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.08.2)
Generated on 2023-10-19
### User Tools

- Add unit tests for Dataproc GKE with mock GKE cluster ([#618](https://github.com/NVIDIA/spark-rapids-tools/pull/618))
- Add support in user tools for running qualification on Dataproc GKE ([#612](https://github.com/NVIDIA/spark-rapids-tools/pull/612))
- [BUG] Update user tools to use latest Databricks CLI version 0.200+ ([#614](https://github.com/NVIDIA/spark-rapids-tools/pull/614))
- Add argprocessor unit test for checking error messages for onprem with no eventlogs ([#605](https://github.com/NVIDIA/spark-rapids-tools/pull/605))
- Updating docs for custom speedup factors for scale factor ([#604](https://github.com/NVIDIA/spark-rapids-tools/pull/604))
- [FEA] Add qualification user tool options to support external pricing ([#595](https://github.com/NVIDIA/spark-rapids-tools/pull/595))
- [DOC] Add documentation for qualification user tool pricing discount options ([#596](https://github.com/NVIDIA/spark-rapids-tools/pull/596))
- [FEA] Add user qualification tool options for specifying pricing discounts for CPU or GPU cluster, or both ([#583](https://github.com/NVIDIA/spark-rapids-tools/pull/583))
- Add diagnostic capabilities for Databricks (AWS/Azure) environments ([#533](https://github.com/NVIDIA/spark-rapids-tools/pull/533))
- Add verbose option to the CLI ([#550](https://github.com/NVIDIA/spark-rapids-tools/pull/550))
- [FEA] Remove URLs from pydantic error messages ([#560](https://github.com/NVIDIA/spark-rapids-tools/pull/560))
- Rename and change pyrapids to spark_rapids_tools ([#570](https://github.com/NVIDIA/spark-rapids-tools/pull/570))
- Fix sdk_monitor exception thrown by abfs protocol ([#569](https://github.com/NVIDIA/spark-rapids-tools/pull/569))

### Core

- Generating speedup factors for Dataproc GKE L4 GPU instances ([#617](https://github.com/NVIDIA/spark-rapids-tools/pull/617))
- Qualification tool: Add penalty for row conversions ([#471](https://github.com/NVIDIA/spark-rapids-tools/pull/471))
- Add support in core tools for running qualification on Dataproc GKE ([#613](https://github.com/NVIDIA/spark-rapids-tools/pull/613))
- Sync up remaining updated execs and exprs from rapids-plugin ([#602](https://github.com/NVIDIA/spark-rapids-tools/pull/602))
- Adding speedup factors for Dataproc Serverless and docs fix ([#603](https://github.com/NVIDIA/spark-rapids-tools/pull/603))
- Add xxhash64 function as supported in qualification tools ([#597](https://github.com/NVIDIA/spark-rapids-tools/pull/597))
- Fix ProjectExecParser to include digits in expression names ([#592](https://github.com/NVIDIA/spark-rapids-tools/pull/592))
- [FEA] Add json_tuple function as supported in qualification tool ([#589](https://github.com/NVIDIA/spark-rapids-tools/pull/589))
- [FEA] Add flatten function as supported in qualification tool ([#587](https://github.com/NVIDIA/spark-rapids-tools/pull/587))
- [FEA] Sync up conv function with rapids-plugin resources ([#573](https://github.com/NVIDIA/spark-rapids-tools/pull/573))

### Miscellaneous

- Bump urllib3 from 1.26.17 to 1.26.18 in /data_validation ([#622](https://github.com/NVIDIA/spark-rapids-tools/pull/622))
- Bump urllib3 from 1.26.14 to 1.26.17 in /data_validation ([#606](https://github.com/NVIDIA/spark-rapids-tools/pull/606))
- Ignore pylint errors to fix python tests ([#611](https://github.com/NVIDIA/spark-rapids-tools/pull/611))


<br/>

## Release [v23.08.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.08.1)
Generated on 2023-09-12
### User Tools

- [DOC] Fix help command in documentation ([#540](https://github.com/NVIDIA/spark-rapids-tools/pull/540))
- Implement a cross-CSP storage driver ([#485](https://github.com/NVIDIA/spark-rapids-tools/pull/485))
- Build tools package as single artifact for restricted environments ([#516](https://github.com/NVIDIA/spark-rapids-tools/pull/516))

### Core

- Remove memoryOverhead recommendations for Standalone Spark ([#557](https://github.com/NVIDIA/spark-rapids-tools/pull/557))
- [FEA] Add support to TIMESTAMP functions ([#549](https://github.com/NVIDIA/spark-rapids-tools/pull/549))
- Fix handling of current_database and ArrayBuffer ([#556](https://github.com/NVIDIA/spark-rapids-tools/pull/556))
- Add `translate` as supported expression in qualification tools ([#546](https://github.com/NVIDIA/spark-rapids-tools/pull/546))
- Adding TakeOrderedAndProject and BroadcastNestedLoopJoin, removing Project from speedup generation ([#548](https://github.com/NVIDIA/spark-rapids-tools/pull/548))
- Qualification should treat promote_precision as supported ([#545](https://github.com/NVIDIA/spark-rapids-tools/pull/545))
- Improve tool error message for files with text extensions ([#544](https://github.com/NVIDIA/spark-rapids-tools/pull/544))
- Improve parsing of aggregate expressions ([#535](https://github.com/NVIDIA/spark-rapids-tools/pull/535))
- Bump default build to use Spark-333 ([#537](https://github.com/NVIDIA/spark-rapids-tools/pull/537))
- Improve AutoTuner plugin recommendation for Fat mode ([#543](https://github.com/NVIDIA/spark-rapids-tools/pull/543))
- Updating speedup generation for more execs from NDS + validation script ([#530](https://github.com/NVIDIA/spark-rapids-tools/pull/530))
- [FEA] Reset speedup factors for qualification tool in EMR 6.12 environments ([#529](https://github.com/NVIDIA/spark-rapids-tools/pull/529))
- Add min, median and max columns to AccumProfileResults ([#522](https://github.com/NVIDIA/spark-rapids-tools/pull/522))
- [FEA] Reset speedup factors for qualification tool in Databricks 12.2 environments ([#524](https://github.com/NVIDIA/spark-rapids-tools/pull/524))
- Filter parser should check ignored-functions ([#520](https://github.com/NVIDIA/spark-rapids-tools/pull/520))
- Update speedup factors for qualification tool in Dataproc 2.1 environments ([#509](https://github.com/NVIDIA/spark-rapids-tools/pull/509))

### Miscellaneous

- Changing max_value to total based on profiler core changes ([#555](https://github.com/NVIDIA/spark-rapids-tools/pull/555))
- Add platform encoding to plugins defined in pom ([#526](https://github.com/NVIDIA/spark-rapids-tools/pull/526))


<br/>

## Release [v23.08.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.08.0)
Generated on 2023-08-25
### User Tools

- Support offline execution of user tools in restricted environments ([#497](https://github.com/NVIDIA/spark-rapids-tools/pull/497))
- Handle deprecation errors in python packaging ([#513](https://github.com/NVIDIA/spark-rapids-tools/pull/513))
- Adds profiling support for EMR in user tools. ([#500](https://github.com/NVIDIA/spark-rapids-tools/pull/500))

### Core

- Fix unit-tests for Spark-340 and Add spark-versions to gh-workflow ([#503](https://github.com/NVIDIA/spark-rapids-tools/pull/503))

### Miscellaneous

- fix gh-workflow for Python unit-tests ([#505](https://github.com/NVIDIA/spark-rapids-tools/pull/505))
- Refactoring the speedup factor generation to support WholeStageCodegen parsing and environment defaults ([#493](https://github.com/NVIDIA/spark-rapids-tools/pull/493))
- Try fix push issue in release action [skip ci] ([#495](https://github.com/NVIDIA/spark-rapids-tools/pull/495))
- Revert "Push to protected branch using third-party action (#492)" ([#494](https://github.com/NVIDIA/spark-rapids-tools/pull/494))
- Push to protected branch using third-party action ([#492](https://github.com/NVIDIA/spark-rapids-tools/pull/492))
- Add secrets in the release.yml ([#491](https://github.com/NVIDIA/spark-rapids-tools/pull/491))
- Add sign-off and token in release workflow ([#490](https://github.com/NVIDIA/spark-rapids-tools/pull/490))


<br/>

## Release [v23.06.4](https://github.com/NVIDIA/spark-rapids-tools/tree/v23.06.4)
Generated on 2023-08-16
### User Tools

- Creating custom speedup factors README with generation script ([#488](https://github.com/NVIDIA/spark-rapids-tools/pull/488))
- Bump dev-version to 23.06.4 ([#468](https://github.com/NVIDIA/spark-rapids-tools/pull/468))

### Core

- [FEA] Enhance qualification tool to handle custom speedup factor file as input ([#475](https://github.com/NVIDIA/spark-rapids-tools/pull/475))
- Bump dev-version to 23.06.4 ([#468](https://github.com/NVIDIA/spark-rapids-tools/pull/468))

### Miscellaneous

- Add Github workflow to automate changelog and release pages ([#486](https://github.com/NVIDIA/spark-rapids-tools/pull/486))
- Fix negative GPU speedup in the core qualification module ([#487](https://github.com/NVIDIA/spark-rapids-tools/pull/487))
- Verify the integrity the downloaded dependencies ([#470](https://github.com/NVIDIA/spark-rapids-tools/pull/470))

