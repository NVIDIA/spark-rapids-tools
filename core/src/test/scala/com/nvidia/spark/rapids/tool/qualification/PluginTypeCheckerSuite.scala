/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tool.qualification

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.nvidia.spark.rapids.tool._
import com.nvidia.spark.rapids.tool.planparser.DataWritingCommandExecParser
import org.scalatest.FunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.TrampolineUtil

class PluginTypeCheckerSuite extends FunSuite with Logging {

  test("read not supported datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:boolean,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,BOOLEAN\n"
      val supText = (header + "parquet,read,NS\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 0.0)
      assert(nsTypes.contains("boolean"))
    }
  }

  test("invalid file") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val header = "Format,Direction,BOOLEAN\n"
      // text longer then header should throw
      val supText = (header + "parquet,read,NS,NS\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      assertThrows[IllegalStateException] {
        checker.setPluginDataSourceFile(csvSupportedFile.toString)
      }
    }
  }

  test("read not CO datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,int\n"
      val supText = (header + "parquet,read,CO\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 0.0)
      assert(nsTypes.contains("int"))
    }
  }

  test("unknown file format") {
    val checker = new PluginTypeChecker
    val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
    val (score, nsTypes) = checker.scoreReadDataTypes("invalidFormat", testSchema)
    assert(score == 0.0)
    assert(nsTypes.contains("*"))
  }

  test("unknown datatype ok") {
    val checker = new PluginTypeChecker
    // right now we only look for unsupported types so an unknown one
    // comes back 1.0
    val testSchema = "loan_id:invalidDT"
    val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
    assert(score == 1.0)
    assert(nsTypes.isEmpty)
  }

  test("supported type") {
    // expect string and bigint parquet to be always supported
    val checker = new PluginTypeChecker
    val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
    val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
    assert(score == 1.0)
    assert(nsTypes.isEmpty)
  }

  test("supported operator score") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val header = "CPUOperator,Score\n"
      val supText = (header + "UnionExec,3\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testScore.txt")
      Files.write(csvSupportedFile, supText)
      checker.setOperatorScore(csvSupportedFile.toString)
      assert(checker.getSpeedupFactor("UnionExec") == 3)
      assert(checker.getSpeedupFactor("ProjectExec") == 1)
    }
  }

  test("supported operator score from default file") {
    val checker = new PluginTypeChecker
    assert(checker.getSpeedupFactor("UnionExec") == 3.0)
    assert(checker.getSpeedupFactor("Ceil") == 4)
  }

  test("supported Execs") {
    val checker = new PluginTypeChecker
    assert(checker.isExecSupported("ShuffledHashJoinExec"))
    assert(checker.isExecSupported("ShuffledHashJoinExec"))
    assert(checker.isExecSupported("CollectLimitExec") == false)
  }

  test("supported Expressions") {
    val checker = new PluginTypeChecker
    val result = checker.getSupportedExprs
    assert(result.contains("add"))
    assert(result("add").equals(OpSuppLevel.S))
    assert(result.contains("isnull"))
  }

  test("write data format"){
    val inputString = Array("Execute InsertIntoHadoopFsRelationCommand " +
      "file:/home/ubuntu/eventlogs/complex_nested_decimal, false," +
      " Parquet, Map(path -> complex_nested_decimal), Append, [name, subject]",
      "Execute InsertIntoHadoopFsRelationCommand gs://08f3844/, " +
      "false, [col1#25, col2#26], ORC, Map(path -> gs://08f3844/), " +
      "Overwrite, [col12, col13, col14]",
      "Execute InsertIntoHadoopFsRelationCommand file:/home/ubuntu/eventlogs/orc-writer-7," +
      "false, [a#7, b#8], ORC, [__partition_columns=[\"a\"], " +
      "path=/home/ubuntu/eventlogs/orc-writer-7], Overwrite, [a, b]")

    val result = inputString.map(DataWritingCommandExecParser.getWriteFormatString(_))
    assert(result(0) == "Parquet")
    assert(result(1) == "ORC")
    assert(result(2) == "ORC")
  }

  val platformSpeedupEntries: Seq[(Platform, Map[String, Double])] = Seq(
    (new OnPremPlatform(Some(A100Gpu)), Map("UnionExec" -> 3.0, "Ceil" -> 4.0)),
    (new DataprocPlatform(Some(T4Gpu)), Map("UnionExec" -> 4.88, "Ceil" -> 4.88)),
    (new EmrPlatform(Some(T4Gpu)), Map("UnionExec" -> 2.07, "Ceil" -> 2.07)),
    (new DatabricksAwsPlatform(Some(T4Gpu)), Map("UnionExec" -> 2.45, "Ceil" -> 2.45)),
    (new DatabricksAzurePlatform(Some(T4Gpu)),
      Map("UnionExec" -> 2.73, "Ceil" -> 2.73)),
    (new DataprocServerlessPlatform(Some(L4Gpu)),
      Map("WindowExec" -> 4.25, "Ceil" -> 4.25)),
    (new DataprocPlatform(Some(L4Gpu)), Map("UnionExec" -> 4.16, "Ceil" -> 4.16)),
    (new DataprocGkePlatform(Some(T4Gpu)), Map("WindowExec" -> 3.65, "Ceil" -> 3.65)),
    (new DataprocGkePlatform(Some(L4Gpu)), Map("WindowExec" -> 3.74, "Ceil" -> 3.74)),
    (new EmrPlatform(Some(A10Gpu)), Map("UnionExec" -> 2.59, "Ceil" -> 2.59))
  )

  platformSpeedupEntries.foreach { case (platform, speedupMap) =>
    test(s"supported operator score from $platform") {
      val checker = new PluginTypeChecker(platform)
      speedupMap.foreach { case (operator, speedup) =>
        assert(checker.getSpeedupFactor(operator) == speedup)
      }
    }
  }

  test("supported operator score from custom speedup factor file") {
    // Using databricks azure speedup factor as custom file
    val platform = PlatformFactory.createInstance(PlatformNames.DATABRICKS_AZURE)
    val speedupFactorFile = ToolTestUtils.getTestResourcePath(platform.getOperatorScoreFile)
    val checker = new PluginTypeChecker(speedupFactorFile=Some(speedupFactorFile))
    assert(checker.getSpeedupFactor("SortExec") == 13.11)
    assert(checker.getSpeedupFactor("FilterExec") == 3.14)
  }

  test("read TOFF datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,int\n"
      val supText = (header + "parquet,read,TOFF\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 0.0)
      assert(nsTypes.contains("int"))
    }
  }

  test("read TNEW datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,int\n"
      val supText = (header + "parquet,read,TNEW\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 0.0)
      assert(nsTypes.contains("int"))
    }
  }

  test("read TON datatype") {
    val checker = new PluginTypeChecker
    TrampolineUtil.withTempDir { outpath =>
      val testSchema = "loan_id:bigint,monthly_reporting_period:string,servicer:string"
      val header = "Format,Direction,int\n"
      val supText = (header + "parquet,read,TON\n").getBytes(StandardCharsets.UTF_8)
      val csvSupportedFile = Paths.get(outpath.getAbsolutePath, "testDS.txt")
      Files.write(csvSupportedFile, supText)
      checker.setPluginDataSourceFile(csvSupportedFile.toString)
      val (score, nsTypes) = checker.scoreReadDataTypes("parquet", testSchema)
      assert(score == 1.0)
      assert(nsTypes.isEmpty)
    }
  }
}
