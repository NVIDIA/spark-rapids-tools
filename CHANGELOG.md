
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

