package gw.cda.api.outputwriter

import com.guidewire.cda.config.ClientConfig

import java.sql.Connection

object MergedConnectionPool {
 var connectionPool: Option[PooledDataSource] = Option.empty

  def init(clientConfig: ClientConfig) = this.synchronized({
      if (connectionPool.isEmpty) {
        connectionPool = Option(new PooledDataSource(clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword))
      }
  })

  def newConnection(): Connection = {
    connectionPool.map(_.newConnection).getOrElse({ throw new IllegalStateException()})
  }
}
