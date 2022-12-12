package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import org.apache.spark.SparkContext

/**
 * Metrics source for the Hikari Connection Pool.
 */
class HikariMetricsSource(c: SparkContext) extends Source {
  override val sourceName: String = "%s.HikariMetrics".format(c.appName)
  override val metricRegistry: MetricRegistry = new MetricRegistry
}
