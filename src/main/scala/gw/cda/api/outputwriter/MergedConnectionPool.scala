package gw.cda.api.outputwriter

import com.guidewire.cda.config.ClientConfig
import org.apache.spark.metrics.source.HikariMetricsSource

import java.sql.Connection

object MergedConnectionPool {
 var connectionPool: Option[PooledDataSource] = Option.empty

  def init(clientConfig: ClientConfig, hikariMetricsSource: HikariMetricsSource): Unit = this.synchronized({
      if (connectionPool.isEmpty) {
        connectionPool = Option(new PooledDataSource(clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword, "merged_connection", clientConfig.connectionPoolSettings))
        connectionPool.foreach(pool => pool.setMeterRegistry(hikariMetricsSource.metricRegistry))
      }
  })

  def newConnection(): Connection = {
    connectionPool.map(_.newConnection).getOrElse({ throw new IllegalStateException()})
  }
}
