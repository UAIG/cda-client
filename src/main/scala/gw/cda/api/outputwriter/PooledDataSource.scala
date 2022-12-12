package gw.cda.api.outputwriter

import com.codahale.metrics.MetricRegistry
import com.guidewire.cda.config.ConnectionPoolSettings
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import java.sql.{Connection, SQLException}

/**
 * Provides JDBC connections from a connection pool with the given configuration.
 *
 * @param jdbcUrl JDBC connection url.
 * @param username JDBC connection username.
 * @param password JDBC connection password.
 * @param poolName Name to assign to the connection pool.
 * @param connectionPoolSettings Additional properties for the connection pool.
 */
class PooledDataSource(val jdbcUrl: String, val username: String, val password: String, poolName: String, connectionPoolSettings: ConnectionPoolSettings) {

  // initialize config
  private val config = new HikariConfig

  config.setJdbcUrl(jdbcUrl)
  config.setUsername(username)
  config.setPassword(password)
  config.addDataSourceProperty("cachePrepStmts", String.valueOf(connectionPoolSettings.cachePrepStmts))
  config.addDataSourceProperty("prepStmtCacheSize", String.valueOf(connectionPoolSettings.prepStmtCacheSize))
  config.addDataSourceProperty("prepStmtCacheSqlLimit", String.valueOf(connectionPoolSettings.prepStmtCacheSqlLimit))
  config.addDataSourceProperty("autoCommit", false)
  if (connectionPoolSettings.idleTimeoutMs > -1L) {
    config.setIdleTimeout(connectionPoolSettings.idleTimeoutMs)
  }
  if (connectionPoolSettings.connectionTimeout > -1L) {
    config.setConnectionTimeout(connectionPoolSettings.connectionTimeout)
  }
  if (connectionPoolSettings.connectionInitSql != "") {
    config.setConnectionInitSql(connectionPoolSettings.connectionInitSql)
  }
  if (connectionPoolSettings.connectionTestQuery != "") {
    config.setConnectionTestQuery(connectionPoolSettings.connectionTestQuery)
  }
  if (connectionPoolSettings.maximumPoolSize > -1) {
    config.setMaximumPoolSize(connectionPoolSettings.maximumPoolSize)
  }
  if (connectionPoolSettings.maxLifetime > -1L) {
    config.setMaxLifetime(connectionPoolSettings.maxLifetime)
  }
  if (connectionPoolSettings.minimumIdle > -1) {
    config.setMinimumIdle(connectionPoolSettings.minimumIdle)
  }
  if (connectionPoolSettings.transactionIsolation != "") {
    config.setTransactionIsolation(connectionPoolSettings.transactionIsolation)
  }
  config.setPoolName(poolName)

  // create the pooled data source
  private val dataSource = new HikariDataSource(config)

  /**
   * Obtain a connection from the pool.
   *
   * @throws SQLException On connection errors or if the pool is exhausted.
   * @return A JDBC connection from the pool.
   */
  @throws[SQLException]
  def newConnection: Connection = dataSource.getConnection

  /**
   * Configure metrics collection for connection pool.
   *
   * @param metricRegistry Meter registry to use.
   */
  def setMeterRegistry(metricRegistry: MetricRegistry): Unit = dataSource.setMetricRegistry(metricRegistry)

}
