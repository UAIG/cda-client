package gw.cda.api.outputwriter

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import java.sql.{Connection, SQLException}

class PooledDataSource(val jdbcUrl: String, val username: String, val password: String) {
  private val config = new HikariConfig

  config.setJdbcUrl(jdbcUrl)
  config.setUsername(username)
  config.setPassword(password)
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  config.addDataSourceProperty("autoCommit", false)
  private val ds = new HikariDataSource(config)

  @throws[SQLException]
  def newConnection: Connection = ds.getConnection

}
