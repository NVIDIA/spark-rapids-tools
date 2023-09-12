
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

