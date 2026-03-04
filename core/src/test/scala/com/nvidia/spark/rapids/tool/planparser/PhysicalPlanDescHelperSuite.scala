/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import org.scalatest.funsuite.AnyFunSuite


class PhysicalPlanDescHelperSuite extends AnyFunSuite {

  // scalastyle:off line.size.limit

  // A multi-node physicalPlanDescription for Iceberg MERGE INTO (copy-on-write)
  private val cowPhysPlanDesc =
    """(12) ReplaceData
      |Input [16]: [_c0#523, _c1#524, _c2#525, _c3#526L, _c4#527, _c5#528]
      |Arguments: IcebergWrite(table=spark_catalog.default.my_target_table, format=PARQUET)
      |
      |(11) Project
      |Output [16]: [_c0#494, _c1#495, _c2#496]
      |Input [36]: [_c0#478, _c1#479]
      |
      |(10) MergeRows
      |Input [36]: [_c0#478, _c1#479, _c2#480]
      |Arguments: isnotnull(__row_from_source#522), [keep(true, _c0#494), keep(true, _c1#495)]
      |""".stripMargin

  // A physicalPlanDescription for merge-on-read (WriteDelta)
  private val morPhysPlanDesc =
    """(16) WriteDelta
      |Input [21]: [__row_operation#10023, _c0#10024, _c1#10025]
      |Arguments: org.apache.iceberg.spark.source.SparkPositionDeltaWrite@5c5feaaa
      |
      |(15) Exchange
      |Input [21]: [__row_operation#10023, _c0#10024]
      |
      |(14) MergeRows
      |Input [36]: [_c0#478, _c1#479]
      |Arguments: isnotnull(__row_from_source#522), [keep(true, _c0#494)]
      |""".stripMargin

  test("extractArgumentsForNode — single-line Arguments for ReplaceData") {
    val result = PhysicalPlanDescHelper.extractArgumentsForNode(cowPhysPlanDesc, "ReplaceData")
    assert(result.isDefined, "Should find ReplaceData Arguments")
    assert(result.get ==
      "IcebergWrite(table=spark_catalog.default.my_target_table, format=PARQUET)")
  }

  test("extractArgumentsForNode — single-line Arguments for WriteDelta") {
    val result = PhysicalPlanDescHelper.extractArgumentsForNode(morPhysPlanDesc, "WriteDelta")
    assert(result.isDefined, "Should find WriteDelta Arguments")
    assert(result.get ==
      "org.apache.iceberg.spark.source.SparkPositionDeltaWrite@5c5feaaa")
  }

  test("extractArgumentsForNode — single-line Arguments for MergeRows") {
    val result = PhysicalPlanDescHelper.extractArgumentsForNode(cowPhysPlanDesc, "MergeRows")
    assert(result.isDefined, "Should find MergeRows Arguments")
    assert(result.get ==
      "isnotnull(__row_from_source#522), [keep(true, _c0#494), keep(true, _c1#495)]")
  }

  test("extractArgumentsForNode — node not found returns None") {
    val result = PhysicalPlanDescHelper.extractArgumentsForNode(cowPhysPlanDesc, "NonExistentNode")
    assert(result.isEmpty, "Should return None for non-existent node")
  }

  test("extractArgumentsForNode — node exists but no Arguments returns None") {
    // Exchange node has no Arguments section
    val result = PhysicalPlanDescHelper.extractArgumentsForNode(morPhysPlanDesc, "Exchange")
    assert(result.isEmpty, "Should return None when node has no Arguments")
  }

  test("extractArgumentsForNode — occurrence parameter selects correct instance") {
    // physPlanDesc with two MergeRows nodes (hypothetical scenario)
    val twoMergeRowsPlan =
      """(20) MergeRows
        |Input [10]: [_c0#100]
        |Arguments: first_merge_args
        |
        |(18) Project
        |Output [5]: [col#200]
        |
        |(15) MergeRows
        |Input [10]: [_c0#300]
        |Arguments: second_merge_args
        |""".stripMargin

    val first = PhysicalPlanDescHelper.extractArgumentsForNode(twoMergeRowsPlan, "MergeRows", 0)
    assert(first.isDefined && first.get == "first_merge_args",
      s"occurrence=0 should return first MergeRows, got $first")

    val second = PhysicalPlanDescHelper.extractArgumentsForNode(twoMergeRowsPlan, "MergeRows", 1)
    assert(second.isDefined && second.get == "second_merge_args",
      s"occurrence=1 should return second MergeRows, got $second")

    val third = PhysicalPlanDescHelper.extractArgumentsForNode(twoMergeRowsPlan, "MergeRows", 2)
    assert(third.isEmpty, "occurrence=2 should return None (only 2 instances)")
  }

  test("extractArgumentsForNode — section bounded correctly when next node has multi-word name") {
    // Operator names can contain spaces (e.g., "SortMergeJoin FullOuter").
    // The section boundary must detect these headers so that Arguments from
    // a later node don't bleed into the current node's section.
    val plan =
      """(12) ReplaceData
        |Input [2]: [a#1, b#2]
        |
        |(9) SortMergeJoin FullOuter
        |Input [4]: [c#3, d#4]
        |Arguments: should_not_be_found
        |""".stripMargin

    // ReplaceData has no Arguments in its section — the Arguments line belongs to SortMergeJoin
    val result = PhysicalPlanDescHelper.extractArgumentsForNode(plan, "ReplaceData")
    assert(result.isEmpty,
      "Should not bleed into SortMergeJoin FullOuter's Arguments")

    // SortMergeJoin FullOuter's own Arguments should be found
    val smjResult = PhysicalPlanDescHelper.extractArgumentsForNode(plan, "SortMergeJoin FullOuter")
    assert(smjResult.isDefined && smjResult.get == "should_not_be_found")
  }

  test("extractArgumentsForNode — empty physPlanDesc returns None") {
    val result = PhysicalPlanDescHelper.extractArgumentsForNode("", "ReplaceData")
    assert(result.isEmpty)
  }

  test("extractArgumentsForNode — multi-line Arguments for MergeRows") {
    val multiLinePlan =
      """(10) MergeRows
        |Input [4]: [_c0#478, _c1#479]
        |Arguments: isnotnull(__row_from_source#522),
        |           isnotnull(__row_from_target#520),
        |           [keep(true, _c0#494)],
        |           [discard(condition)]
        |
        |(11) Project""".stripMargin

    val result = PhysicalPlanDescHelper.extractArgumentsForNode(multiLinePlan, "MergeRows")
    assert(result.isDefined, "Should find multi-line MergeRows Arguments")
    // Should contain all argument lines
    assert(result.get.contains("isnotnull(__row_from_source#522)"))
    assert(result.get.contains("keep(true, _c0#494)"))
    assert(result.get.contains("discard(condition)"))
  }

  // scalastyle:on line.size.limit
}
