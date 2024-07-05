package org.apache.spark.sql.rapids.tool.util

import java.lang.management.ManagementFactory

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * Utility class to track memory metrics.
 * This class is used to track memory metrics such as GC count, GC time,
 * heap memory usage, etc.
 *
 */
class MemoryMetricsTracker {
  private val startGCMetrics = getCurrentGCMetrics

  private def getCurrentGCMetrics: (Long, Long) = {
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

    (gcBeans.map(_.getCollectionCount).sum,
      gcBeans.map(_.getCollectionTime).sum)
  }

  def getTotalGCCount: Long = {
    val (newGcCount:Long, _) = getCurrentGCMetrics
    newGcCount - startGCMetrics._1
  }

  def getTotalGCTime: Long = {
    val (_, newGcTime:Long) = getCurrentGCMetrics
    newGcTime - startGCMetrics._2
  }
}
