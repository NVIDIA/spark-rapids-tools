/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.util.{CacheablePropsHandler, StringUtils}


/**
 * Helper object to extract stable table/scan names and metadata from BatchScan node descriptions.
 *
 * This object provides utility methods to parse BatchScan execution nodes (both CPU and GPU
 * variants) and extract important metadata such as table names, filters, and file locations.
 * The implementation uses node.desc instead of node.name to ensure consistency across
 * different Spark versions.
 *
 * BatchScan nodes appear in Spark plans when reading from:
 * - DataSource V2 tables (Iceberg, Delta Lake, etc.)
 * - File-based data sources using the new DataSource V2 API
 *
 * The helper handles both regular BatchScan and GpuBatchScan nodes.
 */
object BatchScanExtractorHelper {
  // The following substring indicates a BatchScan operation. i.e., "GpuBatchScan", and "BatchScan"
  val OPERATOR_KEYWORD: String = "BatchScan"
  // Pattern to match BatchScan description format:
  // "BatchScan <tableName>[schema] <tableName> (branch=...)" or
  // "GpuBatchScan <tableName>[schema] <tableName> (branch=...)"
  // We extract the table name that appears after "BatchScan" or "GpuBatchScan" and before the
  // schema brackets
  private val batchScanPattern: Regex =
    """^(?:Gpu)?BatchScan\s+([^\[\]]+?)\s*\[.*""".r

  // Pattern to extract filters from the description.
  // The following pattern mainly appears in BatchScan applied on table.
  // Matches: [filters=<filterContent>, groupedBy=...] RuntimeFilters: [<runtimeFilterContent>]
  // Use non-greedy match (.*?) and look for ", groupedBy=" to properly handle commas in filters
  private val filtersPatternForTables: Regex =
    """.*\[filters=(.*?),\s*groupedBy=[^]]*]\s*RuntimeFilters:\s*\[([^]]*)].*""".r

  /**
   * Case class to hold extracted filter information from a BatchScan node.
   *
   * This class encapsulates both static filters (applied at plan time) and runtime filters
   * (applied during execution based on dynamic conditions like dynamic partition pruning).
   *
   * @param filters The static filters applied to the scan (empty string if no filters).
   *                Example: "ss_sold_date_sk IS NOT NULL, ss_store_sk IS NOT NULL"
   * @param runtimeFilters The runtime dynamic filters applied (empty string if no runtime filters).
   *                       Example: "dynamicpruningexpression(ss_sold_date_sk#1834L IN ...)"
   */
  case class BatchScanFiltersForTables(
    filters: String,
    runtimeFilters: String) {

    /**
     * Converts the filter information to a tags map compatible with ReadMetaData.
     *
     * This method creates a map that conforms to the ReadParser metadata tag format:
     * - DataFilters: Set to INAPPLICABLE (not available for table-based BatchScan)
     * - PartitionFilters: Set to INAPPLICABLE (not available for table-based BatchScan)
     * - PushedFilters: Combined filters and runtime filters in format
     *                  "filters=[...],runtimeFilters=[...]"
     *
     * @return Map of metadata tag names to their values
     */
    def toTagsMap: Map[String, String] = {
      val tags = scala.collection.immutable.Map[String, String](
        ReadParser.METAFIELD_TAG_DATA_FILTERS -> StringUtils.INAPPLICABLE_EXTRACT,
        ReadParser.METAFIELD_TAG_PARTITION_FILTERS -> StringUtils.INAPPLICABLE_EXTRACT,
        ReadParser.METAFIELD_TAG_PUSHED_FILTERS ->
          s"filters=[$filters],runtimeFilters=[$runtimeFilters]"
      )
      tags
    }
  }

  /**
   * Extracts the stable name from a BatchScan node description.
   *
   * @param nodeDesc The node description string (typically from SparkPlanGraphNode.desc)
   * @return Some(tableName) if pattern matches, None otherwise
   *
   * Examples:
   * - "BatchScan catalog.database.tableName[id#159...] catalog.database.tableName (branch=null)..."
   *   returns Some("catalog.database.tableName")
   * - "BatchScan catalog.database.tableName[id#16L...] ..."
   *   returns Some("catalog.database.tableName")
   */
  def extractTableName(nodeDesc: String): Option[String] = {
    batchScanPattern.findFirstMatchIn(nodeDesc).map(m => m.group(1).trim)
  }

  /**
   * Extracts filter information from a BatchScan node description.
   *
   * @param nodeDesc The node description string (typically from SparkPlanGraphNode.desc)
   * @return Some(BatchScanFilters) if pattern matches, None otherwise
   *
   * Examples:
   * - "... [filters=, groupedBy=] RuntimeFilters: []"
   *   returns Some(BatchScanFilters("", ""))
   * - "... [filters=f_00 IS NOT NULL, f_00 > 50.00, groupedBy=]
   *   RuntimeFilters: []"
   *   returns Some(BatchScanFilters("f_00 IS NOT NULL, f_00 > 50.00", ""))
   * - "... [filters=f_01 IS NOT NULL, groupedBy=]
   *   RuntimeFilters: [dynamicpruningexpression(f_01#1834L IN dynamicpruning#2440)]"
   *   returns Some(BatchScanFilters("f_01 IS NOT NULL",
   *   "dynamicpruningexpression(f_01#1834L IN dynamicpruning#2440)"))
   */
  def extractFilters(nodeDesc: String): Option[BatchScanFiltersForTables] = {
    filtersPatternForTables.findFirstMatchIn(nodeDesc).map { m =>
      BatchScanFiltersForTables(
        filters = m.group(1).trim,
        runtimeFilters = m.group(2).trim
      )
    }
  }

  /**
   * Extracts the full location path from a BatchScan node name.
   * In recent Spark versions, the BatchScan node name contains the full location:
   * "BatchScan <format> <location>" or "GpuBatchScan <format> <location>"
   *
   * @param nodeName the node name to extract location from
   * @return Some(location) if successfully extracted, None otherwise
   *
   * Examples:
   * - "BatchScan json file:/path/to/file"
   *   returns Some("file:/path/to/file")
   * - "GpuBatchScan parquet hdfs://namenode:8020/user/data/table"
   *   returns Some("hdfs://namenode:8020/user/data/table")
   */
  def extractLocationFromNodeName(nodeName: String): Option[String] = {
    // Pattern to match BatchScan or GpuBatchScan followed by format and location
    // The pattern captures everything after the format (which is typically a single word)
    val locationPattern = """^(?:Gpu)?BatchScan\s+\S+\s+(.+)""".r
    nodeName match {
      case locationPattern(location) => Some(location.trim)
      case _ => None
    }
  }
}

/**
 * Trait for extracting metadata from scan execution nodes.
 *
 * This trait provides the base interface for scan metadata extraction, allowing different
 * implementations to handle various types of scan operations (file-based, table-based, etc.).
 * It also provides utility methods for checking application-level configuration like
 * Iceberg support.
 */
trait ScanMetadataExtractorTrait {
  /**
   * Checks if Iceberg support is enabled in the application.
   *
   * Iceberg is a table format that can be used with BatchScan operations. This method
   * checks the application properties to determine if Iceberg tables are being used.
   * If no app context is provided, it conservatively returns false.
   *
   * @param ap optional CacheablePropsHandler containing application properties
   * @return true if Iceberg is enabled in the application, false otherwise
   */
  def isIcebergEnabled(ap: Option[CacheablePropsHandler]): Boolean = {
    val appEnabled = ap match {
      case None => false  // no app provided then we assume it is false to be safe.
      case Some(a) =>
        // If the app is provided, then check if iceberg is enabled
        a.icebergEnabled
    }
    appEnabled
  }

  /**
   * Extracts metadata from a scan node.
   *
   * This method should be implemented by subtraits/subclasses to extract relevant metadata
   * from different types of scan nodes (BatchScan, FileSourceScan, etc.). The extracted
   * metadata includes information about the data format, location, schema, and filters.
   *
   * @param sNode the SparkPlanGraphNode representing the scan operation
   * @param a optional CacheablePropsHandler containing application-level properties
   * @return ReadMetaData containing extracted scan metadata
   */
  def extractReadMeta(sNode: SparkPlanGraphNode,
      a: Option[CacheablePropsHandler]): ReadMetaData
}

/**
 * Trait for extracting metadata from file-based BatchScan nodes.
 *
 * This trait handles BatchScan operations that read from files (as opposed to tables).
 * It extracts metadata using ReadParser and handles special cases like truncated location
 * paths in the node description.
 *
 * For file-based BatchScan nodes, the description typically contains:
 * - Format information (e.g., "Format: parquet")
 * - Location information (e.g., "Location: InMemoryFileIndex[hdfs://...")
 * - ReadSchema information
 *
 * In recent Spark versions, when the location in the description is truncated (ends with "..."),
 * this trait attempts to extract the full location from the node name which may contain the
 * complete path.
 */
trait BatchScanMetaExtractorTrait extends ScanMetadataExtractorTrait {
  /**
   * Extracts metadata from a file-based BatchScan node.
   *
   * This method performs the following steps:
   * 1. Uses ReadParser to extract basic metadata from the node description
   * 2. Checks if the location is truncated (ends with "...")
   * 3. If truncated, attempts to extract the full location from the node name
   * 4. Returns ReadMetaData with the corrected location
   *
   * The node name format in recent Spark versions: "BatchScan &lt;format&gt; &lt;location&gt;"
   * Example: "BatchScan parquet hdfs://namenode:8020/path/to/data"
   *
   * @param sNode the SparkPlanGraphNode representing the BatchScan operation
   * @param a optional CacheablePropsHandler (not used for file-based scans)
   * @return ReadMetaData with extracted and potentially corrected metadata
   */
  override def extractReadMeta(sNode: SparkPlanGraphNode,
      a: Option[CacheablePropsHandler]): ReadMetaData = {
    // Fallback to default extraction using ReadParser
    // In some recent spark versions the BatchScan will contain the full location of file
    // the format of the nodeName would be like
    // BatchScan <format> <location>
    // if this is the case, then we can get the full name from the node name in case the location
    // is truncated.
    val readMeta = ReadParser.parseReadNode(sNode)

    // Check if the location is truncated (ends with "...") in the description
    // If so, try to extract the full location from the node name
    val finalLocation = if (readMeta.location.endsWith("...")) {
      BatchScanExtractorHelper.extractLocationFromNodeName(sNode.name).getOrElse(readMeta.location)
    } else {
      readMeta.location
    }

    // Return the ReadMetaData with the potentially corrected location
    readMeta.copy(location = finalLocation)
  }
}

/**
 * Trait for extracting metadata from table-based BatchScan nodes.
 *
 * This trait handles BatchScan operations that read from catalog tables (e.g., Iceberg, Delta Lake)
 * as opposed to direct file reads. Table-based BatchScan nodes have different metadata structure
 * and don't contain explicit format/location/schema tags in their descriptions.
 *
 * For table-based BatchScan nodes, the description typically contains:
 * - Table name (e.g., "catalog.database.table")
 * - Filter information in the format: [filters=..., groupedBy=...] RuntimeFilters: [...]
 * - No explicit Format, Location, or ReadSchema tags
 *
 * The format is inferred based on application context
 * (e.g., if Iceberg is enabled, assume Iceberg).
 */
trait BatchScanMetaExtractorFromTableTrait extends BatchScanMetaExtractorTrait {
  /**
   * Extracts metadata from a table-based BatchScan node.
   *
   * This method performs the following steps:
   * 1. Extracts the table name from the node description
   * 2. Determines the table format based on application configuration:
   *    - If Iceberg is enabled, assumes "Iceberg" format
   *    - Otherwise, format is UNKNOWN
   * 3. Extracts filter information (static filters and runtime filters)
   * 4. Returns ReadMetaData with:
   *    - location = table name
   *    - format = determined format
   *    - schema = empty (not available for table scans)
   *    - tags = extracted filter information
   *
   * Note: We cannot rely on node name alone because different Spark versions use different
   * representations. The description parsing is more reliable.
   *
   * @param sNode the SparkPlanGraphNode representing the BatchScan operation
   * @param a optional CacheablePropsHandler containing application properties
   *          (used for Iceberg check)
   * @return ReadMetaData with extracted table metadata
   */
  override def extractReadMeta(sNode: SparkPlanGraphNode,
      a: Option[CacheablePropsHandler]): ReadMetaData = {
    // we cannot rely on nodename alone because different spark versions changed the representation
    // of nodename. So we need to parse the description.
    val (tableName, tblFormat) =
      BatchScanExtractorHelper.extractTableName(sNode.desc) match {
        case Some(tbl) =>
          // Now, decide on the format of the table
          if (isIcebergEnabled(a)) {
            // we cannot extract the format from the batchScan node alone.
            // Optimistically assume that this is supported (parquet)
            (tbl, ReadParser.finalizeFormat("Iceberg", sNode))
          } else {
            (tbl, StringUtils.UNKNOWN_EXTRACT)
          }
        case None => (StringUtils.UNKNOWN_EXTRACT, StringUtils.UNKNOWN_EXTRACT)
      }
    // We cannot know the format from the tableName
    val filterTags = BatchScanExtractorHelper.extractFilters(sNode.desc) match {
      case Some(filters) => filters.toTagsMap
      case None => ReadParser.DEFAULT_METAFIELD_MAP
    }
    ReadMetaData(schema = "", location = tableName, format = tblFormat, tags = filterTags)
  }
}

/**
 * Singleton object implementing table-based BatchScan metadata extraction.
 *
 * This object provides a concrete implementation of BatchScanMetaExtractorFromTableTrait
 * for extracting metadata from BatchScan nodes that read from catalog tables.
 * It can be used as a reusable extractor instance throughout the codebase.
 */
object BatchScanMetaExtractorFromTable extends BatchScanMetaExtractorFromTableTrait {

}

/**
 * Singleton object implementing file-based BatchScan metadata extraction.
 *
 * This object provides a concrete implementation of BatchScanMetaExtractorTrait
 * for extracting metadata from BatchScan nodes that read from files.
 * It can be used as a reusable extractor instance throughout the codebase.
 */
object BatchScanMetaExtractor extends BatchScanMetaExtractorTrait {

}

/**
 * Parser for file-based BatchScan execution nodes.
 *
 * This class handles parsing of BatchScan nodes that read data from files using DataSource V2 API.
 * BatchScan is the execution node used by Spark's DataSource V2 framework for reading data from
 * various file formats (Parquet, ORC, JSON, CSV, etc.) and modern table formats
 * (Delta Lake, Iceberg).
 *
 * Key features:
 * - Extracts metadata about the data source (format, location, schema, filters)
 * - Calculates GPU speedup potential based on format and data type compatibility
 * - Handles both CPU BatchScan and GpuBatchScan nodes
 * - Supports extraction of full file paths when descriptions are truncated
 *
 * Note: BatchScan nodes typically don't have timing metrics, so duration will be None.
 *
 * @param node the SparkPlanGraphNode representing the BatchScan operation
 * @param checker plugin type checker for determining GPU support and speedup factors
 * @param sqlID the SQL query ID this scan belongs to
 * @param app optional AppBase containing application context
 */
class BatchScanExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends BaseSourceScanExecParser(
    node = node,
    checker = checker,
    sqlID = sqlID,
    execName = Some("BatchScanExec"),
    app = app
) with Logging with BatchScanMetaExtractorTrait {
  /**
   * The actual execution name used for pattern matching.
   * This is used to extract the scan token from the node name.
   */
  protected val actualExecName = BatchScanExtractorHelper.OPERATOR_KEYWORD

  /**
   * Regular expression to extract "BatchScan" from the node name.
   * Overrides the base class regex to specifically match "BatchScan" prefix.
   * Pattern: `^\s*(BatchScan).*`
   */
  override val nodeNameRegeX: Regex = s"""^\\s*($actualExecName).*""".r

  /**
   * Metadata extracted from the BatchScan node using file-based extraction logic.
   * This overrides the base class to use the BatchScanMetaExtractorTrait implementation
   * which handles truncated locations and file-based scans.
   */
  override lazy val readInfo: ReadMetaData = extractReadMeta(node, app)

  /**
   * The scan node token extracted from the node name.
   * If pattern matching succeeds, returns "BatchScan", otherwise falls back to actualExecName.
   * This token is used as part of the reported execution name.
   */
  override lazy val scanNodeToken: String = nodeNameRegeX.findFirstMatchIn(trimmedNodeName) match {
    case Some(m) => m.group(1)
    // in case not found, use the full exec name
    case None => actualExecName
  }

  /**
   * Parse the BatchScan execution node and generate ExecInfo.
   *
   * This method delegates to parseNonRDDScan from the base class which:
   * 1. Computes execution duration (typically None for BatchScan)
   * 2. Parses expressions and identifies unsupported ones
   * 3. Determines GPU support and calculates speedup
   * 4. Creates ExecInfo with all collected information
   *
   * @return ExecInfo containing analysis results for this BatchScan operation
   */
  override def parse: ExecInfo = {
    parseNonRDDScan
  }
}

/**
 * Parser for table-based BatchScan execution nodes.
 *
 * This case class extends BatchScanExecParser to handle BatchScan operations that read from
 * catalog tables (e.g., Iceberg, Delta Lake) rather than direct file paths. It uses
 * BatchScanMetaExtractorFromTableTrait to extract metadata specific to table scans.
 *
 * Differences from file-based BatchScan:
 * - No explicit Format, Location, or ReadSchema tags in node description
 * - Table name is used as the location
 * - Format is inferred from application configuration (e.g., Iceberg if enabled)
 * - Filter information is extracted from [filters=..., groupedBy=...] RuntimeFilters: [...] pattern
 *
 * The parser automatically detects whether a BatchScan is table-based or file-based by checking
 * for the presence of metadata field tags in the node description.
 *
 * @param node the SparkPlanGraphNode representing the table-based BatchScan operation
 * @param checker plugin type checker for determining GPU support and speedup factors
 * @param sqlID the SQL query ID this scan belongs to
 * @param app optional AppBase containing application context (used for Iceberg detection)
 */
case class BatchScanFromTableExecParser(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    override val app: Option[AppBase]
) extends BatchScanExecParser (
    node = node,
    checker = checker,
    sqlID = sqlID,
    app = app
) with Logging with BatchScanMetaExtractorFromTableTrait {

}

/**
 * Companion object for BatchScanExecParser providing factory methods and utility functions.
 *
 * This object implements the GroupParserTrait to integrate with the parser framework.
 * It provides methods to:
 * - Determine if a node is a BatchScan operation
 * - Distinguish between table-based and file-based BatchScan nodes
 * - Create appropriate parser instances based on the node type
 * - Extract metadata from BatchScan nodes
 *
 * The object uses a set of metadata field tags to differentiate between table-based and
 * file-based scans. File-based scans contain tags like "Format:", "Location:", and "ReadSchema:"
 * in their descriptions, while table-based scans do not.
 */
object BatchScanExecParser extends GroupParserTrait {
  /**
   * Set of metadata field tags that indicate a file-based BatchScan.
   * If a node description contains any of these tags, it's considered a file-based scan.
   */
  val lookupMetaFields = Set(
    ReadParser.METAFIELD_TAG_LOCATION,
    ReadParser.METAFIELD_TAG_FORMAT,
    ReadParser.METAFIELD_TAG_READ_SCHEMA)

  /**
   * Checks if the given node name represents a BatchScan operation.
   *
   * @param nodeName the name of the execution node
   * @return true if the node name contains "BatchScan", false otherwise
   */
  override def accepts(nodeName: String): Boolean = {
    nodeName.contains(BatchScanExtractorHelper.OPERATOR_KEYWORD)
  }

  /**
   * Checks if the given node name represents a BatchScan operation (with configuration).
   * This variant accepts a configuration provider but delegates to the simpler accepts method.
   *
   * @param nodeName the name of the execution node
   * @param confProvider optional configuration provider (unused)
   * @return true if the node name contains "BatchScan", false otherwise
   */
  override def accepts(
      nodeName: String,
      confProvider: Option[CacheablePropsHandler]): Boolean = {
    accepts(nodeName)
  }

  /**
   * Checks if the given node represents a BatchScan operation.
   * This variant accepts the full node and configuration but delegates to name-based checking.
   *
   * @param node the SparkPlanGraphNode to check
   * @param confProvider optional configuration provider (unused)
   * @return true if the node represents a BatchScan operation, false otherwise
   */
  override def accepts(
      node: SparkPlanGraphNode,
      confProvider: Option[CacheablePropsHandler]): Boolean = {
    accepts(node.name, confProvider)
  }

  /**
   * Creates an appropriate ExecParser for a BatchScan node.
   *
   * This factory method determines whether the BatchScan is table-based or file-based
   * and creates the corresponding parser instance:
   * - BatchScanFromTableExecParser for table-based scans
   * - BatchScanExecParser for file-based scans
   *
   * The determination is made by checking if the node description contains any of the
   * metadata field tags in lookupMetaFields. If none are found, it's a table-based scan.
   *
   * @param node the SparkPlanGraphNode representing the BatchScan operation
   * @param checker plugin type checker for GPU support analysis
   * @param sqlID the SQL query ID this scan belongs to
   * @param execName optional execution name override (unused)
   * @param opType optional operation type override (unused)
   * @param app optional AppBase containing application context
   * @return ExecParser instance appropriate for the BatchScan type
   */
  override def createExecParser(
      node: SparkPlanGraphNode,
      checker: PluginTypeChecker,
      sqlID: Long,
      execName: Option[String] = None,
      opType: Option[OpTypes.Value] = None,
      app: Option[AppBase] = None): ExecParser = {
    // Decide whether this is batch scan from table or from file
    if (isBatScanFromTable(node)) {
      // Fallback to batch scan from table
      BatchScanFromTableExecParser(
        node = node,
        checker = checker,
        sqlID = sqlID,
        app = app)

    } else {
      new BatchScanExecParser(
        node = node,
        checker = checker,
        sqlID = sqlID,
        app = app)
    }
  }

  /**
   * Determines if a BatchScan node is table-based (as opposed to file-based).
   *
   * This method checks if any of the metadata field tags (Format, Location, ReadSchema)
   * are present in the node description. If none are found, the node is considered a
   * table-based scan.
   *
   * Logic:
   * - File-based scans contain tags like "Format: parquet", "Location: hdfs://...", etc.
   * - Table-based scans don't have these tags, only table name and filter information
   *
   * @param node the SparkPlanGraphNode to check
   * @return true if this is a table-based BatchScan, false if file-based
   */
  def isBatScanFromTable(node: SparkPlanGraphNode): Boolean = {
    // Decide whether this is batch scan from table or from file
    !lookupMetaFields.exists(node.desc.contains(_))
  }

  /**
   * Extracts metadata from a BatchScan node using the appropriate extractor.
   *
   * This method determines if the BatchScan is table-based or file-based and delegates
   * to the appropriate metadata extractor:
   * - BatchScanMetaExtractorFromTable for table-based scans
   * - BatchScanMetaExtractor for file-based scans
   *
   * @param node the SparkPlanGraphNode representing the BatchScan operation
   * @param confProvider optional CacheablePropsHandler for application properties
   * @return ReadMetaData containing extracted metadata (format, location, schema, filters)
   */
  def extractReadMetaData(
      node: SparkPlanGraphNode,
      confProvider: Option[CacheablePropsHandler] = None): ReadMetaData = {
    if (isBatScanFromTable(node)) {
      BatchScanMetaExtractorFromTable.extractReadMeta(node, confProvider)
    } else {
      BatchScanMetaExtractor.extractReadMeta(node, confProvider)
    }
  }
}
