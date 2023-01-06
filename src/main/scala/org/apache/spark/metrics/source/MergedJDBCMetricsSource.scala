package org.apache.spark.metrics.source

import com.codahale.metrics.{Counter, Histogram, MetricRegistry}
import org.apache.spark.SparkContext

/**
 * Metrics source for merged JDBC updates.
 */
class MergedJDBCMetricsSource(c: SparkContext) extends Source {
  override val sourceName: String = "%s.MergedJDBCMetrics".format(c.appName)
  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * Number of records within timestamp folders.
   */
  val timestamp_record_count_history: Histogram = metricRegistry.histogram(MetricRegistry.name("timestamp_folder.record_count"))

  /**
   * Number of statements in executed JDBC batches.
   */
  val jdbc_batch_size_history: Histogram = metricRegistry.histogram(MetricRegistry.name("batch_size"))

  /**
   * Counts the number of rows that are effected in the database by executed insert statements. 
   */
  val rows_inserted_counter: Counter = metricRegistry.counter(MetricRegistry.name("rows.inserted_count"))

  /**
   * Counts the number of rows that are effected in the database by executed delete statements. 
   */
  val rows_deleted_counter: Counter = metricRegistry.counter(MetricRegistry.name("rows.deleted_count"))

  /**
   * Counts the number of rows that are effected in the database by executed update statements. 
   */
  val rows_updated_counter: Counter = metricRegistry.counter(MetricRegistry.name("rows.updated_count"))

  /**
   * Counts the number of insert statements that are executed. 
   */
  val insert_statement_counter: Counter = metricRegistry.counter(MetricRegistry.name("statements.insert_count"))

  /**
   * Counts the number of delete statements that are executed. 
   */
  val delete_statement_counter: Counter = metricRegistry.counter(MetricRegistry.name("statements.delete_count"))

  /**
   * Counts the number of update statements that are executed. 
   */
  val update_statement_counter: Counter = metricRegistry.counter(MetricRegistry.name("statements.update_count"))

  /**
   * Histogram of mismatches between executed insert statements and affected rows in the database.
   * A positive value indicates that more statements were executed than rows updated, a negative value indicates that more rows were updated than statements executed.
   */
  val insert_affect_row_mismatch_history: Histogram = metricRegistry.histogram(MetricRegistry.name("affected_rows.insert_mismatches"))

  /**
   * Histogram of mismatches between executed update statements and affected rows in the database.
   * A positive value indicates that more statements were executed than rows updated, a negative value indicates that more rows were updated than statements executed.
   */
  val update_affect_row_mismatch_history: Histogram = metricRegistry.histogram(MetricRegistry.name("affected_rows.update_mismatches"))

  /**
   * Histogram of mismatches between executed update statements and affected rows in the database.
   * A positive value indicates that more statements were executed than rows updated, a negative value indicates that more rows were updated than statements executed.
   */
  val delete_affect_row_mismatch_history: Histogram = metricRegistry.histogram(MetricRegistry.name("affected_rows.delete_mismatches"))

  /**
   * Counts the number of create table statements that are executed. 
   */
  val create_table_counter: Counter = metricRegistry.counter(MetricRegistry.name("statements.create_count"))

  /**
   * Counts the number of alter statements that are executed. 
   */
  val alter_table_counter: Counter = metricRegistry.counter(MetricRegistry.name("statements.alter_count"))

  /**
   * Counts the number of errors while updating the database. 
   */
  val data_write_error_counter: Counter = metricRegistry.counter(MetricRegistry.name("data.write_error_count"))

  /**
   * Histogram of mismatches between batch-metrics.json and executed updates in a table timestamp folder.
   * A positive value indicates that more updates were expected by the batch-metrics.json than rows updated,
   * a negative value indicates that more rows were updated than expected by the batch-metrics.json.
   */
  val batch_metrics_mismatched_rows: Histogram = metricRegistry.histogram(MetricRegistry.name("batch_metrics.mismatched_rows"))

  /**
   * A counter for the number of table timestamp folders with batch-metrics.json row update count mismatches.
   */
  val batch_metrics_mismatched_timestamp_folder_count: Counter = metricRegistry.counter(MetricRegistry.name("batch_metrics.mismatched_timestamp_folder_count"))

}
