
<br/>

## Release [v24.12.2](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.12.2)
Generated on 2025-01-31
### User Tools

- Revert "Follow Up: Make '--platform' argument mandatory in CLI (#1473)" ([#1498](https://github.com/NVIDIA/spark-rapids-tools/pull/1498))
- Fix Hadoop JAR Download Timeouts in Behave Tests ([#1503](https://github.com/NVIDIA/spark-rapids-tools/pull/1503))

### Core

- AutoTuner: Set recommendation for spark.task.resource.gpu.amount to a very low value ([#1514](https://github.com/NVIDIA/spark-rapids-tools/pull/1514))
- [FEA] Add IO diagnostic output for GPU slowness in Profiler tool ([#1451](https://github.com/NVIDIA/spark-rapids-tools/pull/1451))
- [BUG] Qual tool should convert time units at stage/job/sql level ([#1511](https://github.com/NVIDIA/spark-rapids-tools/pull/1511))
- Fix string comparison for memory overhead in pinned pool size recommendation in AutoTuner ([#1508](https://github.com/NVIDIA/spark-rapids-tools/pull/1508))
- Update core tools rules to allow cross-build between 2.12 and 2.13 ([#1510](https://github.com/NVIDIA/spark-rapids-tools/pull/1510))
- Sync plugin support as of 2024-12-31 ([#1478](https://github.com/NVIDIA/spark-rapids-tools/pull/1478))
- Add stringType and binaryType to the list of dataType map ([#1506](https://github.com/NVIDIA/spark-rapids-tools/pull/1506))
- [BUG] Remove duplicated executor CPU time and runtime metric from SQLTaskAggMetricsProfileResult ([#1504](https://github.com/NVIDIA/spark-rapids-tools/pull/1504))
- Improve AutoTuner cluster configuration recommendations for GPU runs ([#1501](https://github.com/NVIDIA/spark-rapids-tools/pull/1501))

### Miscellaneous

- Use common add-to-project action [skip ci] ([#1505](https://github.com/NVIDIA/spark-rapids-tools/pull/1505))


<br/>

## Release [v24.12.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.12.1)
Generated on 2025-01-14
### User Tools

- Add compute_precision_recall utility function ([#1500](https://github.com/NVIDIA/spark-rapids-tools/pull/1500))
- Fix additional FutureWarning issues ([#1499](https://github.com/NVIDIA/spark-rapids-tools/pull/1499))
- Qualx model updates from weekly KPI run 2025-01-10 ([#1495](https://github.com/NVIDIA/spark-rapids-tools/pull/1495))
- Fix future warnings for pandas>=2.2 ([#1494](https://github.com/NVIDIA/spark-rapids-tools/pull/1494))
- Pin scikit-learn dependency for shap ([#1491](https://github.com/NVIDIA/spark-rapids-tools/pull/1491))
- Make spill heuristic 1 TB by default ([#1488](https://github.com/NVIDIA/spark-rapids-tools/pull/1488))
- Support Python 3.9-3.12 ([#1486](https://github.com/NVIDIA/spark-rapids-tools/pull/1486))
- Update models for latest code/datasets ([#1485](https://github.com/NVIDIA/spark-rapids-tools/pull/1485))

### Core

- Improve scalastyle rules to detect spaces ([#1493](https://github.com/NVIDIA/spark-rapids-tools/pull/1493))
- Improve shuffle manager recommendation in AutoTuner with version validation ([#1483](https://github.com/NVIDIA/spark-rapids-tools/pull/1483))
- Support group-limit optimization for ROW_NUMBER in Qualification ([#1487](https://github.com/NVIDIA/spark-rapids-tools/pull/1487))
-  Bump minimum Spark version to 3.2.0 and improve AutoTuner unit tests for multiple Spark versions  ([#1482](https://github.com/NVIDIA/spark-rapids-tools/pull/1482))
- Fix inconsistent shuffle write time sum results in Profiler output ([#1450](https://github.com/NVIDIA/spark-rapids-tools/pull/1450))
- Refine Qualification AutoTuner recommendations for shuffle partitions for CPU event logs ([#1479](https://github.com/NVIDIA/spark-rapids-tools/pull/1479))
- Split AutoTuner for Profiling and Qualification and Override BATCH_SIZE_BYTES ([#1471](https://github.com/NVIDIA/spark-rapids-tools/pull/1471))


<br/>

## Release [v24.12.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.12.0)
Generated on 2024-12-20
### User Tools

- Make '--platform' argument mandatory in qualification and profiling CLI to prevent incorrect behavior ([#1463](https://github.com/NVIDIA/spark-rapids-tools/pull/1463))

### Core

- Skip processing apps with invalid platform and spark runtime configurations ([#1421](https://github.com/NVIDIA/spark-rapids-tools/pull/1421))
- Improve implementation of finding median in StatisticsMetrics ([#1474](https://github.com/NVIDIA/spark-rapids-tools/pull/1474))
- Optimize implementation of getAggregateRawMetrics in core-tools ([#1468](https://github.com/NVIDIA/spark-rapids-tools/pull/1468))
- Adding Spark 3.5.2 support in auto tuner for EMR ([#1466](https://github.com/NVIDIA/spark-rapids-tools/pull/1466))
- Mark RunningWindowFunction as supported in Qual tool ([#1465](https://github.com/NVIDIA/spark-rapids-tools/pull/1465))
- Deduplicate calls to aggregateSparkMetricsBySql ([#1464](https://github.com/NVIDIA/spark-rapids-tools/pull/1464))

### Miscellaneous

- Follow Up: Make '--platform' argument mandatory in CLI ([#1473](https://github.com/NVIDIA/spark-rapids-tools/pull/1473))


<br/>

## Release [v24.10.3](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.10.3)
Generated on 2024-12-13
### User Tools

- Fix dataframe handling of column-types ([#1458](https://github.com/NVIDIA/spark-rapids-tools/pull/1458))


<br/>

## Release [v24.10.2](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.10.2)
Generated on 2024-12-06
### User Tools

- Update models for latest tools code ([#1448](https://github.com/NVIDIA/spark-rapids-tools/pull/1448))
- More flexible regexes; fix default split function ([#1443](https://github.com/NVIDIA/spark-rapids-tools/pull/1443))
- Update models for latest code and dataset JSON ([#1442](https://github.com/NVIDIA/spark-rapids-tools/pull/1442))
- Add model for databricks-azure_photon and update combined model ([#1427](https://github.com/NVIDIA/spark-rapids-tools/pull/1427))
- Remove custom-speedup module from user-tools ([#1425](https://github.com/NVIDIA/spark-rapids-tools/pull/1425))

### Core

- Count expressions per Exec in SQLPlanParser ([#1449](https://github.com/NVIDIA/spark-rapids-tools/pull/1449))
- Report all operators in the output file ([#1444](https://github.com/NVIDIA/spark-rapids-tools/pull/1444))
- Fix missing exec-to-stageId mapping in Qual tool ([#1437](https://github.com/NVIDIA/spark-rapids-tools/pull/1437))
- [BUG] Fix Profiler tool index out of bound exception when generating diagnostic metrics ([#1439](https://github.com/NVIDIA/spark-rapids-tools/pull/1439))
- Sort Qual execs report by sqlId and nodeId ([#1436](https://github.com/NVIDIA/spark-rapids-tools/pull/1436))
- Include expression parsers for HashAggregate and ObjectHashAggregate ([#1432](https://github.com/NVIDIA/spark-rapids-tools/pull/1432))
- [FEA] Add stage/task level diagnostic output for GPU slowness in Profiler tool ([#1375](https://github.com/NVIDIA/spark-rapids-tools/pull/1375))
- Reduce the log noise caused by core report summary ([#1426](https://github.com/NVIDIA/spark-rapids-tools/pull/1426))
- Trigger GC at the beginning of each benchmark iteration ([#1424](https://github.com/NVIDIA/spark-rapids-tools/pull/1424))

### Miscellaneous

- [BUG] Fix sync plugin files script to handle empty or non-existing cvs files ([#1446](https://github.com/NVIDIA/spark-rapids-tools/pull/1446))
- Enable license header check ([#1440](https://github.com/NVIDIA/spark-rapids-tools/pull/1440))


<br/>

## Release [v24.10.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.10.1)
Generated on 2024-11-15
### User Tools

- Add qualification support for Photon jobs in the Python Tool ([#1409](https://github.com/NVIDIA/spark-rapids-tools/pull/1409))
- Add qualx support for platform runtime variants (DB AWS) ([#1417](https://github.com/NVIDIA/spark-rapids-tools/pull/1417))
- Update models for latest emr, onprem eventlogs ([#1410](https://github.com/NVIDIA/spark-rapids-tools/pull/1410))

### Core

- Adding EMR-specific tunings for shuffle manager and ignoring jar ([#1419](https://github.com/NVIDIA/spark-rapids-tools/pull/1419))
- Changing autotuner memory error to warning in comments ([#1418](https://github.com/NVIDIA/spark-rapids-tools/pull/1418))
- Add sparkRuntime property to capture runtime type in application_information ([#1414](https://github.com/NVIDIA/spark-rapids-tools/pull/1414))
- Refactor Exec Parsers - remove individual parser classes ([#1396](https://github.com/NVIDIA/spark-rapids-tools/pull/1396))
- Remove estimated GPU duration from qualification output ([#1412](https://github.com/NVIDIA/spark-rapids-tools/pull/1412))


<br/>

## Release [v24.10.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.10.0)
Generated on 2024-11-04
### User Tools

- [FEA] Allow users to specify custom Dependency jars ([#1395](https://github.com/NVIDIA/spark-rapids-tools/pull/1395))
- Reduce default memory allocation to the java process ([#1407](https://github.com/NVIDIA/spark-rapids-tools/pull/1407))
- Update error handling in python for parsing cluster information ([#1394](https://github.com/NVIDIA/spark-rapids-tools/pull/1394))
- user-tools should add xms argument to java cmd ([#1391](https://github.com/NVIDIA/spark-rapids-tools/pull/1391))
- Use environment variables to set thresholds in static yaml configurations ([#1389](https://github.com/NVIDIA/spark-rapids-tools/pull/1389))
- Use StorageLib to download dependencies ([#1383](https://github.com/NVIDIA/spark-rapids-tools/pull/1383))
- Remove total core second heuristic and filter apps only in top candidate view ([#1376](https://github.com/NVIDIA/spark-rapids-tools/pull/1376))
- Generate log files for Python Profiling cli ([#1366](https://github.com/NVIDIA/spark-rapids-tools/pull/1366))
- Update models for updated datasets and latest code ([#1365](https://github.com/NVIDIA/spark-rapids-tools/pull/1365))
- Isolate dataset for qualx plugin invocations ([#1361](https://github.com/NVIDIA/spark-rapids-tools/pull/1361))
- [FEA] Add total core seconds into top candidate view ([#1342](https://github.com/NVIDIA/spark-rapids-tools/pull/1342))
- Fix python tool picking up wrong JAR version in Fat wheel mode ([#1357](https://github.com/NVIDIA/spark-rapids-tools/pull/1357))
- [FOLLOWUP-1326] Set Spark version to 3.4.2 by default for onprem environment ([#1358](https://github.com/NVIDIA/spark-rapids-tools/pull/1358))
- Disable `too-many-positional-arguments` in pylintrc ([#1353](https://github.com/NVIDIA/spark-rapids-tools/pull/1353))
- Reduce console output tree level, exclude JAR tool output files and remove incorrect logging ([#1340](https://github.com/NVIDIA/spark-rapids-tools/pull/1340))

### Core

- Add support for Photon-specific SQL Metrics ([#1390](https://github.com/NVIDIA/spark-rapids-tools/pull/1390))
- Add support for processing Photon event logs in Scala ([#1338](https://github.com/NVIDIA/spark-rapids-tools/pull/1338))
- Add Reflection to support custom Spark Implementation at Runtime ([#1362](https://github.com/NVIDIA/spark-rapids-tools/pull/1362))
- Improve AQE support by capturing SQLPlan versions ([#1354](https://github.com/NVIDIA/spark-rapids-tools/pull/1354))
- Add PartitionFilters and DataFilters to the dataSourceInfo table ([#1346](https://github.com/NVIDIA/spark-rapids-tools/pull/1346))
- Add support to ArrayJoin in Qualification tool ([#1345](https://github.com/NVIDIA/spark-rapids-tools/pull/1345))

### Miscellaneous

- Cluster information should handle dynamic allocation and nodes being removed and added ([#1369](https://github.com/NVIDIA/spark-rapids-tools/pull/1369))
- Rename tag core to core_tools ([#1350](https://github.com/NVIDIA/spark-rapids-tools/pull/1350))


<br/>

## Release [v24.08.2](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.08.2)
Generated on 2024-09-10
### User Tools

- Add end-to-end behavioural tests for the python CLI ([#1313](https://github.com/NVIDIA/spark-rapids-tools/pull/1313))
- Add documentation for qualx plugins ([#1337](https://github.com/NVIDIA/spark-rapids-tools/pull/1337))
- Allow spark dependency to be configured dynamically ([#1326](https://github.com/NVIDIA/spark-rapids-tools/pull/1326))
- Follow-up 1318: Fix QualX fallback with default speedup and duration columns ([#1330](https://github.com/NVIDIA/spark-rapids-tools/pull/1330))
- Updated models for EMR NDS-H dataset ([#1331](https://github.com/NVIDIA/spark-rapids-tools/pull/1331))

### Core

- [FEA] Add total core seconds in Qualification core tool output ([#1320](https://github.com/NVIDIA/spark-rapids-tools/pull/1320))
- Add support to MaxBy and MinBy in Qualification tool ([#1335](https://github.com/NVIDIA/spark-rapids-tools/pull/1335))
- Add safeguards to prevent older attempts from generating metrics output in Scala Tool ([#1324](https://github.com/NVIDIA/spark-rapids-tools/pull/1324))
- Sync up DAYTIME and YEARMONTH fields with CSV plugin files ([#1328](https://github.com/NVIDIA/spark-rapids-tools/pull/1328))

### Miscellaneous

- Update signoff usage [skip ci] ([#1332](https://github.com/NVIDIA/spark-rapids-tools/pull/1332))


<br/>

## Release [v24.08.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.08.1)
Generated on 2024-09-04
### User Tools

- [DOC] spark_rapids CLI help cmd still shows cost savings ([#1317](https://github.com/NVIDIA/spark-rapids-tools/pull/1317))
- Fix Qualification and Profiling tools CLI argument shorthands ([#1312](https://github.com/NVIDIA/spark-rapids-tools/pull/1312))
- Raise error for enum creation from invalid string values ([#1300](https://github.com/NVIDIA/spark-rapids-tools/pull/1300))
- Append HADOOP_CONF_DIR to the tools CLASSPATH execution cmd ([#1308](https://github.com/NVIDIA/spark-rapids-tools/pull/1308))
- Fix key error and cross-join error during qualx evaluate ([#1298](https://github.com/NVIDIA/spark-rapids-tools/pull/1298))
- Qual tool: Print more useful log messages when failures happen downloading dependencies ([#1292](https://github.com/NVIDIA/spark-rapids-tools/pull/1292))
- Fix --help text for custom_model_file option ([#1285](https://github.com/NVIDIA/spark-rapids-tools/pull/1285))

### Core

- Remove legacy SpeedupFactor from core output files ([#1318](https://github.com/NVIDIA/spark-rapids-tools/pull/1318))
- Mark decimalsum as supported in Qualification tool ([#1323](https://github.com/NVIDIA/spark-rapids-tools/pull/1323))
- Mark SMJ as unsupported operator for corner cases in left join ([#1309](https://github.com/NVIDIA/spark-rapids-tools/pull/1309))
- Remove arguments and code related to the html-report ([#1311](https://github.com/NVIDIA/spark-rapids-tools/pull/1311))
- Handle SparkRapidsBuildInfoEvent in GPU event logs ([#1203](https://github.com/NVIDIA/spark-rapids-tools/pull/1203))
- Enable recursive search for event logs by default and optional `--no-recursion` flag ([#1297](https://github.com/NVIDIA/spark-rapids-tools/pull/1297))
- Qualification tool support filtering by a filesystem time range ([#1299](https://github.com/NVIDIA/spark-rapids-tools/pull/1299))
- Skip generating timeline for stages that do not have completion time ([#1290](https://github.com/NVIDIA/spark-rapids-tools/pull/1290))
- Save core tools logs to output log file ([#1269](https://github.com/NVIDIA/spark-rapids-tools/pull/1269))
- Qualification tool - Add option to filter by minimum event log size ([#1291](https://github.com/NVIDIA/spark-rapids-tools/pull/1291))
- Include exception message for unknown app status in core tool ([#1281](https://github.com/NVIDIA/spark-rapids-tools/pull/1281))

### Miscellaneous

- Remove restricted google sheets link and outdated TCO section ([#1289](https://github.com/NVIDIA/spark-rapids-tools/pull/1289))


<br/>

## Release [v24.08.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.08.0)
Generated on 2024-08-13
### User Tools

- Remove calculation of gpu cluster recommendation from python tool when cluster argument is passed ([#1278](https://github.com/NVIDIA/spark-rapids-tools/pull/1278))
- Remove unused argument `--target_platform` in Python Tool ([#1279](https://github.com/NVIDIA/spark-rapids-tools/pull/1279))
- Qualification tool: Add output stats file for Execs(operators) ([#1225](https://github.com/NVIDIA/spark-rapids-tools/pull/1225))
- Include GPU information in the cluster recommendation for Dataproc and OnPrem ([#1265](https://github.com/NVIDIA/spark-rapids-tools/pull/1265))
- Remove speedup based recommendation column from qual_summary csv ([#1268](https://github.com/NVIDIA/spark-rapids-tools/pull/1268))
- Fix prediction CSV files for multiple qual directories ([#1267](https://github.com/NVIDIA/spark-rapids-tools/pull/1267))
- Clean up tools after removing CLI dependency ([#1256](https://github.com/NVIDIA/spark-rapids-tools/pull/1256))
- Rename cluster shape columns to use 'worker' prefix in the output files and rename metadata file ([#1258](https://github.com/NVIDIA/spark-rapids-tools/pull/1258))
- Remove CLI dependency in Dataproc `_pull_gpu_hw_info` implementation ([#1245](https://github.com/NVIDIA/spark-rapids-tools/pull/1245))
- Replace split_nds with split_train_val ([#1252](https://github.com/NVIDIA/spark-rapids-tools/pull/1252))
- Update xgboost models and metrics ([#1244](https://github.com/NVIDIA/spark-rapids-tools/pull/1244))
- Add footnotes for config recommendations and speedup category in top candidate view ([#1243](https://github.com/NVIDIA/spark-rapids-tools/pull/1243))
- [BUG] Update Dataproc instance catalog for n1 series GPU info ([#1242](https://github.com/NVIDIA/spark-rapids-tools/pull/1242))
- Improvements in Cluster Config Recommender  ([#1241](https://github.com/NVIDIA/spark-rapids-tools/pull/1241))
- Improve console output from python tool for failed/gpu/photon event logs ([#1235](https://github.com/NVIDIA/spark-rapids-tools/pull/1235))
- [FEA] Generate and use instance description file for Databricks-Azure platform ([#1232](https://github.com/NVIDIA/spark-rapids-tools/pull/1232))
- Remove arguments related to cost-savings ([#1230](https://github.com/NVIDIA/spark-rapids-tools/pull/1230))
- Updated models for latest databricks-aws datasets ([#1231](https://github.com/NVIDIA/spark-rapids-tools/pull/1231))
- Refactor QualX for Linter and Test Compatibility  ([#1228](https://github.com/NVIDIA/spark-rapids-tools/pull/1228))
- Generate summary metadata file and fix node recommendation in python  ([#1216](https://github.com/NVIDIA/spark-rapids-tools/pull/1216))
- [FEA] Remove gcloud CLI dependency for Dataproc platform ([#1223](https://github.com/NVIDIA/spark-rapids-tools/pull/1223))
- Updated models for latest dataproc eventlogs ([#1226](https://github.com/NVIDIA/spark-rapids-tools/pull/1226))
- Remove estimation-model column from qualification summary ([#1220](https://github.com/NVIDIA/spark-rapids-tools/pull/1220))
- Add option to add features.csv files to training set ([#1212](https://github.com/NVIDIA/spark-rapids-tools/pull/1212))
- Disable cost saving functionality ([#1218](https://github.com/NVIDIA/spark-rapids-tools/pull/1218))
- [FEA] Remove CLI dependency for EMR and Databricks-AWS platforms in user tool ([#1196](https://github.com/NVIDIA/spark-rapids-tools/pull/1196))
- Fix some basic pylint errors in qualx code ([#1210](https://github.com/NVIDIA/spark-rapids-tools/pull/1210))
- Qual tool tuning rec based on CPU event log coherently recommend tunings and node setup and infer cluster from eventlog ([#1188](https://github.com/NVIDIA/spark-rapids-tools/pull/1188))
- Add shap command to internal CLI for debugging ([#1197](https://github.com/NVIDIA/spark-rapids-tools/pull/1197))
- Add internal CLI to generate instance descriptions for CSPs ([#1137](https://github.com/NVIDIA/spark-rapids-tools/pull/1137))
- [FEA] Support custom XGBoost model file via user tools CLI ([#1184](https://github.com/NVIDIA/spark-rapids-tools/pull/1184))
- Updated models for new training data ([#1186](https://github.com/NVIDIA/spark-rapids-tools/pull/1186))
- Add evaluate_summary command to internal CLI ([#1185](https://github.com/NVIDIA/spark-rapids-tools/pull/1185))
- [DOC] Fix broken link to qualX docs and update python prerequisites ([#1180](https://github.com/NVIDIA/spark-rapids-tools/pull/1180))
- Bump to certifi-2024.7.4 and urllib3-1.26.19 ([#1173](https://github.com/NVIDIA/spark-rapids-tools/pull/1173))
- Disable UI-HTML report by default in Qualification tool ([#1168](https://github.com/NVIDIA/spark-rapids-tools/pull/1168))
- Fix parsing App IDs inside metrics directory in QualX ([#1167](https://github.com/NVIDIA/spark-rapids-tools/pull/1167))
- Refactor Databricks-AWS Qual tool to cache and process pricing info from DB website ([#1141](https://github.com/NVIDIA/spark-rapids-tools/pull/1141))
- Add plugin mechanism for dataset-specific preprocessing in qualx ([#1148](https://github.com/NVIDIA/spark-rapids-tools/pull/1148))
- Unsupported op logic should read action column from qual's output ([#1150](https://github.com/NVIDIA/spark-rapids-tools/pull/1150))
- Update qualx readme for training ([#1140](https://github.com/NVIDIA/spark-rapids-tools/pull/1140))
- Disable pylint-unreachable code in tox.ini ([#1145](https://github.com/NVIDIA/spark-rapids-tools/pull/1145))

### Core

- Include GPU information in the cluster recommendation for Dataproc and OnPrem ([#1265](https://github.com/NVIDIA/spark-rapids-tools/pull/1265))
- [TASK] Optimize the storage of accumulables in core tools ([#1263](https://github.com/NVIDIA/spark-rapids-tools/pull/1263))
- Sync GetJsonObject support with Rapids-Plugin ([#1266](https://github.com/NVIDIA/spark-rapids-tools/pull/1266))
-  Do not create new StageInfo object ([#1261](https://github.com/NVIDIA/spark-rapids-tools/pull/1261))
- [FEA] Add support for `map_from_arrays` in qualification tools ([#1248](https://github.com/NVIDIA/spark-rapids-tools/pull/1248))
- Rename cluster shape columns to use 'worker' prefix in the output files and rename metadata file ([#1258](https://github.com/NVIDIA/spark-rapids-tools/pull/1258))
- Fix stage level metrics output csv file ([#1251](https://github.com/NVIDIA/spark-rapids-tools/pull/1251))
- Handle event logs with wildcards in status report generation ([#1237](https://github.com/NVIDIA/spark-rapids-tools/pull/1237))
- Fix duplicate records in DataSourceInfo report ([#1227](https://github.com/NVIDIA/spark-rapids-tools/pull/1227))
- Reduce memory footprint of stageInfo ([#1222](https://github.com/NVIDIA/spark-rapids-tools/pull/1222))
- Ensure UTF-8 encoding for reading non-english characters ([#1211](https://github.com/NVIDIA/spark-rapids-tools/pull/1211))
- Sync plugin support for hash-hive and shift operators ([#1198](https://github.com/NVIDIA/spark-rapids-tools/pull/1198))
- Sync-up the support of parse_url in qualification tool ([#1195](https://github.com/NVIDIA/spark-rapids-tools/pull/1195))
- Include status information for failed event logs in core tool ([#1187](https://github.com/NVIDIA/spark-rapids-tools/pull/1187))
- [FEA] Adding Benchmarking classes to evaluate core tools performance ([#1169](https://github.com/NVIDIA/spark-rapids-tools/pull/1169))
- [BUG] Fix handling of non-english characters in tools output files ([#1189](https://github.com/NVIDIA/spark-rapids-tools/pull/1189))
- [Bug] Fix java Qual tool handling of `--platform` argument ([#1161](https://github.com/NVIDIA/spark-rapids-tools/pull/1161))
- Add all stage metrics to tools output ([#1151](https://github.com/NVIDIA/spark-rapids-tools/pull/1151))
- Follow-up 1142: remove TODO line ([#1146](https://github.com/NVIDIA/spark-rapids-tools/pull/1146))
- Mark wholestageCodeGen as shouldRemove when child nodes are removed ([#1142](https://github.com/NVIDIA/spark-rapids-tools/pull/1142))
- [FEA] Display full failure messages in failed CSV files ([#1135](https://github.com/NVIDIA/spark-rapids-tools/pull/1135))

### Miscellaneous

- Qualification tool: Add option to filter event logs for a maximum file system size ([#1275](https://github.com/NVIDIA/spark-rapids-tools/pull/1275))
- Qualification tool should print Kryo related recommendations  ([#1204](https://github.com/NVIDIA/spark-rapids-tools/pull/1204))
- Fix header check script to exclude files ([#1224](https://github.com/NVIDIA/spark-rapids-tools/pull/1224))
- Update header check script for pre-commit hooks ([#1219](https://github.com/NVIDIA/spark-rapids-tools/pull/1219))
- Follow-up 1189: handle non-english characters in data-output.js ([#1208](https://github.com/NVIDIA/spark-rapids-tools/pull/1208))
- Update pre-commit hooks to check for headers and white-spaces ([#1205](https://github.com/NVIDIA/spark-rapids-tools/pull/1205))
- user-tools:Update --help for cluster argument ([#1178](https://github.com/NVIDIA/spark-rapids-tools/pull/1178))
- Support fine-tuning models ([#1174](https://github.com/NVIDIA/spark-rapids-tools/pull/1174))


<br/>

## Release [v24.06.1](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.06.1)
Generated on 2024-06-18
### User Tools

- Fix Python runtime error caused by numpy 2.0.0 release ([#1130](https://github.com/NVIDIA/spark-rapids-tools/pull/1130))
- Disable the spark_rapids bootstrap command ([#1114](https://github.com/NVIDIA/spark-rapids-tools/pull/1114))

### Core

- Handle different exception thrown by incomplete eventlogs ([#1124](https://github.com/NVIDIA/spark-rapids-tools/pull/1124))
- Include number of executors per node in cluster information  ([#1119](https://github.com/NVIDIA/spark-rapids-tools/pull/1119))


<br/>

## Release [v24.06.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.06.0)
Generated on 2024-06-12
### User Tools

- Add support to Python 3.12 ([#1111](https://github.com/NVIDIA/spark-rapids-tools/pull/1111))
- user-tools: Update log messages ([#1110](https://github.com/NVIDIA/spark-rapids-tools/pull/1110))
- Enable xgboost prediction model by default ([#1108](https://github.com/NVIDIA/spark-rapids-tools/pull/1108))
- Add support to Python3.11 ([#1105](https://github.com/NVIDIA/spark-rapids-tools/pull/1105))
- Fix nan label issue in training ([#1104](https://github.com/NVIDIA/spark-rapids-tools/pull/1104))
- Fix qualx app metrics ([#1102](https://github.com/NVIDIA/spark-rapids-tools/pull/1102))
- clip appDuration to at least Duration ([#1096](https://github.com/NVIDIA/spark-rapids-tools/pull/1096))
- Fix missing assignment to savings_recommendations ([#1098](https://github.com/NVIDIA/spark-rapids-tools/pull/1098))
- Handle QualX behaviour when Qual Tool does not generate any outputs ([#1095](https://github.com/NVIDIA/spark-rapids-tools/pull/1095))
- Fix internal predict CLI and remove preprocessed argument ([#1093](https://github.com/NVIDIA/spark-rapids-tools/pull/1093))
- Update QualX to return default speedups and fix App Duration for incomplete apps ([#1089](https://github.com/NVIDIA/spark-rapids-tools/pull/1089))
- fix signature error from overlapping merges ([#1084](https://github.com/NVIDIA/spark-rapids-tools/pull/1084))
- sync w/ internal repo; update models ([#1083](https://github.com/NVIDIA/spark-rapids-tools/pull/1083))
- Reduce the maximum number of Java threads in CLI ([#1082](https://github.com/NVIDIA/spark-rapids-tools/pull/1082))
- Remove using Profiler metrics for QualX and Heuristics ([#1080](https://github.com/NVIDIA/spark-rapids-tools/pull/1080))
- Port QualX repo and add CLI for train ([#1076](https://github.com/NVIDIA/spark-rapids-tools/pull/1076))
- User tools fallback to default zone/region ([#1054](https://github.com/NVIDIA/spark-rapids-tools/pull/1054))
- Handle missing pricing info for user qual tool on Databricks platforms ([#1053](https://github.com/NVIDIA/spark-rapids-tools/pull/1053))
- Split job and stage level aggregated metrics into different files ([#1050](https://github.com/NVIDIA/spark-rapids-tools/pull/1050))
- Skip Cluster Inference when CSP CLIs are missing or not configured ([#1035](https://github.com/NVIDIA/spark-rapids-tools/pull/1035))
- Store Cluster Shape Recommendation in User Tools Qualification Output ([#1005](https://github.com/NVIDIA/spark-rapids-tools/pull/1005))
- Fix calculation of unsupported operators stage duration percentage ([#1006](https://github.com/NVIDIA/spark-rapids-tools/pull/1006))
- Update Databricks Azure qual tool to set env variable for ABFS paths ([#1016](https://github.com/NVIDIA/spark-rapids-tools/pull/1016))
- Add heuristics using stage spill metrics to skip apps ([#1002](https://github.com/NVIDIA/spark-rapids-tools/pull/1002))
- Fix failure in github workflow's pylint ([#1015](https://github.com/NVIDIA/spark-rapids-tools/pull/1015))
- Updating qual validation script to directly use top candidate view recommendation ([#1001](https://github.com/NVIDIA/spark-rapids-tools/pull/1001))

### Core

- Fix typo in Profiler class using qual instead of prof ([#1113](https://github.com/NVIDIA/spark-rapids-tools/pull/1113))
- Fix missing appEndTime in raw_metrics folder ([#1092](https://github.com/NVIDIA/spark-rapids-tools/pull/1092))
- Sync tools with plugin newly supported operators ([#1066](https://github.com/NVIDIA/spark-rapids-tools/pull/1066))
- Fix java Qual tool Autotuner output when GPU device is missing ([#1085](https://github.com/NVIDIA/spark-rapids-tools/pull/1085))
- Update the Qual tool AutoTuner Heuristics against CPU event logs ([#1069](https://github.com/NVIDIA/spark-rapids-tools/pull/1069))
- Handling FileNotFound exception in AutoTuner ([#1065](https://github.com/NVIDIA/spark-rapids-tools/pull/1065))
- Handle metric names from legacy spark ([#1052](https://github.com/NVIDIA/spark-rapids-tools/pull/1052))
- Split job and stage level aggregated metrics into different files ([#1050](https://github.com/NVIDIA/spark-rapids-tools/pull/1050))
- Refactor ProfileResult classes to implement new interface design and add CSV output to Qual Tool ([#1043](https://github.com/NVIDIA/spark-rapids-tools/pull/1043))
- Hook up the auto tuner in the qualification tool ([#1039](https://github.com/NVIDIA/spark-rapids-tools/pull/1039))
- Profiler should identify the delta log ops and generate views for non-delta logs ([#1031](https://github.com/NVIDIA/spark-rapids-tools/pull/1031))
- Qualification tool - Handle cancelled jobs and stages better and don't skip the app ([#1033](https://github.com/NVIDIA/spark-rapids-tools/pull/1033))
- [FEA] Generate Status Report for Profiling Tool ([#1012](https://github.com/NVIDIA/spark-rapids-tools/pull/1012))
- Fix calculation of unsupported operators stage duration percentage ([#1006](https://github.com/NVIDIA/spark-rapids-tools/pull/1006))
- Fix potential problems and AQE updates in Qual tool ([#1021](https://github.com/NVIDIA/spark-rapids-tools/pull/1021))
- Sync supported operators with plugin changes and update default score ([#1020](https://github.com/NVIDIA/spark-rapids-tools/pull/1020))
- Refactor TaskEnd to be accessible by Q/P tools ([#1000](https://github.com/NVIDIA/spark-rapids-tools/pull/1000))

### Miscellaneous

- Bump requests from 2.31.0 to 2.32.2 in /data_validation ([#1077](https://github.com/NVIDIA/spark-rapids-tools/pull/1077))


<br/>

## Release [v24.04.0](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.04.0)
Generated on 2024-05-07
### User Tools

- [FEA] Add CLI to run prediction on estimation_model ([#961](https://github.com/NVIDIA/spark-rapids-tools/pull/961))
- Adding SHAP predict values as new output file ([#982](https://github.com/NVIDIA/spark-rapids-tools/pull/982))
- Update docs for building to clarify to build in a virtual environment ([#976](https://github.com/NVIDIA/spark-rapids-tools/pull/976))

### Core

- [BUG] Catch Profiler error when app info is empty ([#994](https://github.com/NVIDIA/spark-rapids-tools/pull/994))
- Get stages from sqlId for collecting info for output writer functions ([#996](https://github.com/NVIDIA/spark-rapids-tools/pull/996))
- Account for joboverhead time in qualification tool estimation ([#992](https://github.com/NVIDIA/spark-rapids-tools/pull/992))
- [Followup] Fix handling of clusterTags and SparkVersion in Q/P Tools ([#993](https://github.com/NVIDIA/spark-rapids-tools/pull/993))
- Fix handling of clusterTags and SparkVersion in Q/P Tools ([#991](https://github.com/NVIDIA/spark-rapids-tools/pull/991))
- Refactor AppBase to use common AppMetaData between Q/P tools ([#983](https://github.com/NVIDIA/spark-rapids-tools/pull/983))
- Refactor Stage info code between Q/P tools ([#971](https://github.com/NVIDIA/spark-rapids-tools/pull/971))


<br/>

## Release [v24.02.4](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.02.4)
Generated on 2024-04-30
### User Tools

- Fix Hadoop Azure version to be compatibe with Spark-3.5.0 ([#975](https://github.com/NVIDIA/spark-rapids-tools/pull/975))
- Add speedup categories in qualification summary output  ([#958](https://github.com/NVIDIA/spark-rapids-tools/pull/958))
- Improve cluster node initialisation for CSPs ([#964](https://github.com/NVIDIA/spark-rapids-tools/pull/964))

### Core

- Remove databricks profiling recommendation for dynamicFilePruning ([#972](https://github.com/NVIDIA/spark-rapids-tools/pull/972))
- Add AQEShuffleRead WriteFiles execs to the supportedOps and score files ([#963](https://github.com/NVIDIA/spark-rapids-tools/pull/963))
- [FEA] Automate appending new operators to the platform score sheets ([#954](https://github.com/NVIDIA/spark-rapids-tools/pull/954))
- Add support for InSubqueryExec Expression ([#960](https://github.com/NVIDIA/spark-rapids-tools/pull/960))

### Miscellaneous

- Bump dev version to 24.02.4 ([#968](https://github.com/NVIDIA/spark-rapids-tools/pull/968))
- Revert versions back to 24.02.3 ([#967](https://github.com/NVIDIA/spark-rapids-tools/pull/967))


<br/>

## Release [v24.02.3](https://github.com/NVIDIA/spark-rapids-tools/tree/v24.02.3)
Generated on 2024-04-24
### User Tools

- Cache CLI calls for node instance description ([#952](https://github.com/NVIDIA/spark-rapids-tools/pull/952))
- Improve error handling in prediction code ([#950](https://github.com/NVIDIA/spark-rapids-tools/pull/950))
- Support dynamic calculation of JVM resources in CLI cmd ([#944](https://github.com/NVIDIA/spark-rapids-tools/pull/944))
- Syncup estimation model prediction logic updates ([#946](https://github.com/NVIDIA/spark-rapids-tools/pull/946))
- Cluster inference should not run for unsupported platform ([#941](https://github.com/NVIDIA/spark-rapids-tools/pull/941))
- Fix invalid values in cluster creation script ([#935](https://github.com/NVIDIA/spark-rapids-tools/pull/935))
- Fix core tool doc links and user qualification tool default argument values ([#931](https://github.com/NVIDIA/spark-rapids-tools/pull/931))
- Fix gpu cluster recommendation in user tools ([#930](https://github.com/NVIDIA/spark-rapids-tools/pull/930))
- Bump idna from 3.4 to 3.7 in /data_validation ([#932](https://github.com/NVIDIA/spark-rapids-tools/pull/932))
- Add cluster details in qualification summary output ([#921](https://github.com/NVIDIA/spark-rapids-tools/pull/921))
- Refactor `find_matches_for_node` return values ([#920](https://github.com/NVIDIA/spark-rapids-tools/pull/920))
- [FEA] Add and use g5 AWS instances as default for qualification tool output ([#898](https://github.com/NVIDIA/spark-rapids-tools/pull/898))
- Add jar argument to spark_rapids CLI ([#902](https://github.com/NVIDIA/spark-rapids-tools/pull/902))
- Support driverlog argument in profiler CLI ([#897](https://github.com/NVIDIA/spark-rapids-tools/pull/897))

### Core

- Followups on handling Photon eventlogs ([#953](https://github.com/NVIDIA/spark-rapids-tools/pull/953))
- Sync operators support timestamped 24-04-16 ([#951](https://github.com/NVIDIA/spark-rapids-tools/pull/951))
- Add CheckOverflowInTableInsert support: verify absence from physical plan ([#942](https://github.com/NVIDIA/spark-rapids-tools/pull/942))
- Fix Notes column in the supported ops CSV files ([#933](https://github.com/NVIDIA/spark-rapids-tools/pull/933))
- Improve sync plugin supported CSV python script ([#919](https://github.com/NVIDIA/spark-rapids-tools/pull/919))
- Add cluster details in qualification summary output ([#921](https://github.com/NVIDIA/spark-rapids-tools/pull/921))
- Add support for unsupported expressions reasons per Exec ([#923](https://github.com/NVIDIA/spark-rapids-tools/pull/923))
- Adding more metrics and options for qual validation ([#926](https://github.com/NVIDIA/spark-rapids-tools/pull/926))
- Generate cluster details in JSON output ([#912](https://github.com/NVIDIA/spark-rapids-tools/pull/912))
- Add Divide and multiple interval expressions as supported ([#917](https://github.com/NVIDIA/spark-rapids-tools/pull/917))
- Add support for PythonMapInArrowExec and MapInArrowExec ([#913](https://github.com/NVIDIA/spark-rapids-tools/pull/913))
- Re-enable support for GetJsonObject by default ([#916](https://github.com/NVIDIA/spark-rapids-tools/pull/916))
- Add support for WindowGroupLimitExec ([#906](https://github.com/NVIDIA/spark-rapids-tools/pull/906))
- [FEA] Skip Spark Structured Streaming event logs for Qualification tool ([#905](https://github.com/NVIDIA/spark-rapids-tools/pull/905))
- [FEA] Add and use g5 AWS instances as default for qualification tool output ([#898](https://github.com/NVIDIA/spark-rapids-tools/pull/898))
- Initial version of qual tool validation script for classification metrics ([#903](https://github.com/NVIDIA/spark-rapids-tools/pull/903))
- Fix Delta-core dependency for Spark35+ ([#904](https://github.com/NVIDIA/spark-rapids-tools/pull/904))
- Add support for AtomicCreateTableAsSelectExec ([#895](https://github.com/NVIDIA/spark-rapids-tools/pull/895))
- Add support for KnownNullable and EphemeralSubstring expressions ([#894](https://github.com/NVIDIA/spark-rapids-tools/pull/894))
- Add Support for BloomFilterAggregate and BloomFilterMightContain exprs ([#891](https://github.com/NVIDIA/spark-rapids-tools/pull/891))
- [DOC] Update README for sync plugin supported ops script ([#893](https://github.com/NVIDIA/spark-rapids-tools/pull/893))
- Add operators to ignore list and update WindowExpr parser ([#890](https://github.com/NVIDIA/spark-rapids-tools/pull/890))
- Add support to RoundCeil and RoundFloor expressions ([#889](https://github.com/NVIDIA/spark-rapids-tools/pull/889))


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

