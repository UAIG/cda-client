package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import org.apache.spark.SparkContext

/**
 * Metrics source for the CDA data load process.
 */
class ReaderMetricsSource(c: SparkContext) extends Source {
  override val sourceName: String = "%s.ReaderMetrics".format(c.appName)
  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * Time it takes to write records for a single timestamp folder.
   */
  val timestamp_write_time_history: Histogram = metricRegistry.histogram(MetricRegistry.name("timestamp_folder.write_time_ms"))

  /**
   * Time it takes to construct a dataframe for all Parquet files within a timestamp folder.
   */
  val timestamp_fetch_time_history: Histogram = metricRegistry.histogram(MetricRegistry.name("timestamp_folder.fetch_time_ms"))

  /**
   * Counter for all "written" record counts from any batch-metrics.json that is being processed.
   */
  val batch_metrics_written_counter: Counter = metricRegistry.counter(MetricRegistry.name("batch_metrics.written_count"))

  /**
   * Counter for all "dropped" record counts from any batch-metrics.json that is being processed.
   */
  val batch_metrics_dropped_counter: Counter = metricRegistry.counter(MetricRegistry.name("batch_metrics.dropped_count"))

  /**
   * Counter for all "read" record counts from any batch-metrics.json that is being processed.
   */
  val batch_metrics_read_counter: Counter = metricRegistry.counter(MetricRegistry.name("batch_metrics.read_count"))

  /**
   * Counts the number of errors while fetching data from S3. 
   */
  val data_read_error_counter: Counter = metricRegistry.counter(MetricRegistry.name("data.read_error_count"))

  /**
   * Counts the number of errors while fetching batch-metrics.json from S3. 
   */
  val batch_metrics_read_error_counter: Counter = metricRegistry.counter(MetricRegistry.name("batch_metrics.read_error_count"))

  /**
   * Histogram of processing time for entire tables in ms. 
   */
  val table_process_time_history: Histogram = metricRegistry.histogram(MetricRegistry.name("table.processing_time_ms"))

  /**
   * Histogram of number of tables found in the manifest.
   */
  val manifest_table_count_history: Histogram = metricRegistry.histogram(MetricRegistry.name("manifest.table_count"))
}
