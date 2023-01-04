package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfig
import com.guidewire.cda.config.InvalidConfigParameterException
import gw.cda.api.outputwriter.StatementType.{StatementType, UPDATE}
import org.apache.spark.metrics.source.MergedJDBCMetricsSource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.storage.StorageLevel

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.time.{Duration, Instant}
import java.util
import java.util.{Calendar, Collections, Locale, TimeZone}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object JdbcWriteType extends Enumeration {
  type JdbcWriteType = Value

  val Raw = Value("Raw")
  val Merged = Value("Merged")
}

object StatementType extends Enumeration {
  type StatementType = Value
  val INSERT, UPDATE, DELETE, RAW = Value
}

case class JdbcUpdateResult(updateStatementCount: Long, updatedRowCount: Long) {}

case class MicroBatchUpdateResult(insertResult: JdbcUpdateResult, updateResult: JdbcUpdateResult, deleteResult: JdbcUpdateResult) {}

private[outputwriter] class JdbcOutputWriter(override val clientConfig: ClientConfig, cdaMergedJDBCMetricsSource: MergedJDBCMetricsSource, outputWriterConfig: OutputWriterConfig) extends OutputWriter {

  private[outputwriter] val configLargeTextFields: Set[String] = Option(clientConfig.outputSettings.largeTextFields).getOrElse("").replaceAll(" ", "").toLowerCase(Locale.US).split(",").toSet
  private val DEFAULT_BATCH_SIZE: Long = 5000L
  protected val batchSize: Long = if (clientConfig.outputSettings.jdbcBatchSize <= 0) DEFAULT_BATCH_SIZE else clientConfig.outputSettings.jdbcBatchSize

  /**
   * Validate DB connection URL and credentials.
   */
  override def validate(): Unit = {
    if (clientConfig.outputSettings.saveIntoJdbcRaw) {
      validateDBConnection(clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword)
    } else if (clientConfig.outputSettings.saveIntoJdbcMerged) {
      validateDBConnection(clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername, clientConfig.jdbcConnectionMerged.jdbcPassword)
    }
  }

  override def write(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch): Unit = {
    val tableName = tableDataFrameWrapperForMicroBatch.tableName

    if (clientConfig.outputSettings.saveIntoJdbcRaw && clientConfig.outputSettings.saveIntoJdbcMerged) {
      // If we are saving to both raw and merged datasets then we need to commit and rollback the
      // connections together since they share a common savepoint.  If either fail then we need to
      // rollback both connections to keep them in sync.
      val rawConn = DriverManager.getConnection(clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword)
      rawConn.setAutoCommit(false) // Everything in the same db transaction.
      val mergedConn = MergedConnectionPool.newConnection()
      mergedConn.setAutoCommit(false)
      try {
        try {
          this.writeJdbcRaw(tableDataFrameWrapperForMicroBatch, rawConn)
        } catch {
          case e: Exception =>
            rawConn.rollback()
            log.info(s"Raw - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
            throw e
        }
        try {
          this.writeJdbcMerged(tableDataFrameWrapperForMicroBatch, mergedConn)
        } catch {
          case e: Exception =>
            cdaMergedJDBCMetricsSource.data_write_error_counter.inc()
            rawConn.rollback()
            log.info(s"Raw - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
            mergedConn.rollback()
            log.info(s"Merged - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
            throw e
        }
        // Commit both connections.
        rawConn.commit()
        mergedConn.commit()
      } finally {
        // Close both connections.
        mergedConn.close()
        rawConn.close()
      }
    } else {
      if (clientConfig.outputSettings.saveIntoJdbcRaw) {
        // Processing raw dataset only.
        val rawConn = DriverManager.getConnection(clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername, clientConfig.jdbcConnectionRaw.jdbcPassword)
        rawConn.setAutoCommit(false)
        try {
          this.writeJdbcRaw(tableDataFrameWrapperForMicroBatch, rawConn)
        } catch {
          case e: Exception =>
            rawConn.rollback()
            rawConn.close()
            log.info(s"Raw - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} - $e - ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
            throw e
        }
        rawConn.commit()
        rawConn.close()
      } else {
        if (clientConfig.outputSettings.saveIntoJdbcMerged) {
          // Processing merged dataset only.
          val mergedConn = MergedConnectionPool.newConnection()
          mergedConn.setAutoCommit(false)
          try {
            this.writeJdbcMerged(tableDataFrameWrapperForMicroBatch, mergedConn)
            mergedConn.commit()
          } catch {
            case e: Exception =>
              cdaMergedJDBCMetricsSource.data_write_error_counter.inc()
              mergedConn.rollback()
              log.warn(s"Merged - ROLLBACK '$tableName' for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint}, timestamp ${tableDataFrameWrapperForMicroBatch.folderTimestamp} - $e - ${clientConfig.jdbcConnectionMerged.jdbcUrl}")
              throw e
          } finally {
            mergedConn.close()
          }
        }
      }
    }
  }

  /**
   * Since all output types share a common savepoints.json,
   * make sure there are no schema change issues for JdbcRaw or
   * JdbcMerged before writing data for this fingerprint to any of the target types.
   */
  override def schemasAreConsistent(dataframe: DataFrame, tableName: String,
                                    schemaFingerprint: String, sparkSession: SparkSession): Boolean = {

    val jdbcRawIsOk = if (clientConfig.outputSettings.saveIntoJdbcRaw) {
      schemasAreConsistent(dataframe, clientConfig.jdbcConnectionRaw.jdbcSchema, tableName, schemaFingerprint,
        clientConfig.jdbcConnectionRaw.jdbcUrl, clientConfig.jdbcConnectionRaw.jdbcUsername,
        clientConfig.jdbcConnectionRaw.jdbcPassword, sparkSession, JdbcWriteType.Raw)
    } else true

    val jdbcMergedIsOk = if (clientConfig.outputSettings.saveIntoJdbcMerged) {
      schemasAreConsistent(dataframe, clientConfig.jdbcConnectionMerged.jdbcSchema, tableName, schemaFingerprint,
        clientConfig.jdbcConnectionMerged.jdbcUrl, clientConfig.jdbcConnectionMerged.jdbcUsername,
        clientConfig.jdbcConnectionMerged.jdbcPassword, sparkSession, JdbcWriteType.Merged)
    } else true

    jdbcRawIsOk && jdbcMergedIsOk
  }

  /**
   * @param jdbcWriteType Merge vs Raw to determine exclusion of internal 'gwcbi_'
   *                      columns when comparing schemas.  When merging data we remove those columns
   *                      from the data set before saving the data so we don't want to check
   *                      for them when comparing to the schema definition in the database.
   */
  protected def schemasAreConsistent(fileDataFrame: DataFrame, jdbcSchemaName: String, tableName: String,
                                     schemaFingerprint: String, url: String, user: String, pswd: String,
                                     spark: SparkSession, jdbcWriteType: JdbcWriteType.Value): Boolean = {
    //Derive the product name from the url to avoid having to create or pass in a connection
    // to access the metadata object.
    val dbProductName = if (url.toLowerCase.contains("sqlserver")) {
      "Microsoft SQL Server"
    } else if (url.toLowerCase.contains("postgresql")) {
      "PostgreSQL"
    } else if (url.toLowerCase.contains("oracle")) {
      "Oracle"
    }

    if (tableExists(tableName, jdbcSchemaName, url, user, pswd)) {
      // build a query that returns no data from the table.  This will still get us the schema definition which is all we need.
      val sql = dbProductName match {
        case "Microsoft SQL Server" | "PostgreSQL" => s"(select * from $jdbcSchemaName.$tableName where 1=2) as $tableName"
        case "Oracle"                              => s"(select * from $jdbcSchemaName.$tableName where 1=2)"
        case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
      }
      val tableDataFrame = spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", sql)
        .option("user", user)
        .option("password", pswd)
        .load()

      val dialect = JdbcDialects.get(url)
      val tableSchemaDef = tableDataFrame.schema
      val fileSchemaDef = if (jdbcWriteType == JdbcWriteType.Merged) {
        val dropList = fileDataFrame.columns.filter(colName => !colName.toLowerCase.equals("gwcbi___seqval_hex") && (colName.toLowerCase.startsWith("gwcbi___") || colName.toLowerCase.startsWith("gwcdac__")))
        fileDataFrame.drop(dropList: _*).schema
      } else {
        fileDataFrame.schema
      }

      val databaseConnection = DriverManager.getConnection(url, user, pswd)
      databaseConnection.setAutoCommit(false)

      var newColumnAdded = false
      //ADD COLUMNS TO DATABASE TABLE THAT HAVE BEEN ADDED TO PARQUET FILE
      // Check to see if there are columns in the parquet file that are not in the database table.
      // If there are we are going to build the ALTER TABLE statement and execute the statement.
      fileSchemaDef.foreach(field => if (!scala.util.Try(tableDataFrame(field.name)).isSuccess) {
        val columnDefinition = buildDDLColumnDefinition(dialect, dbProductName.toString, tableName, field.name, field.dataType, field.nullable)
        val alterTableStatement = s"ALTER TABLE $jdbcSchemaName.$tableName ADD $columnDefinition"
        log.info(s"Statement to be executed: $alterTableStatement")
        try {
          // Execute the table create DDL
          val stmt = databaseConnection.createStatement
          stmt.execute(alterTableStatement)
          stmt.close()
          databaseConnection.commit()
          newColumnAdded = true
          log.info(s"ALTER TABLE - SUCCESS '$tableName' for alter table statement $alterTableStatement - $url")
        } catch {
          case e: Exception =>
            databaseConnection.rollback()
            databaseConnection.close()
            log.warn(s"ALTER TABLE - ROLLBACK '$tableName' for alter table statement $alterTableStatement - $e - $url")
            throw e
        }
        cdaMergedJDBCMetricsSource.alter_table_counter.inc()
      })

      databaseConnection.close()

      // Generate the table create ddl statement based on the schema definition of the database table.
      val databaseDDL = getTableCreateDDL(dialect, tableSchemaDef, tableName, jdbcWriteType, dbProductName.toString)

      // Build the create ddl statement based on the data read from the parquet file.
      val fileDDL = getTableCreateDDL(dialect, fileSchemaDef, tableName, jdbcWriteType, dbProductName.toString)

      //Compare the two table definitions and log warnings if they do not match.
      // Added twist here - we need to check to see if columns had to be added or removed from the database table.
      // If we had to ADD columns to the database table we need to rebuild the dataframe for the JDBC
      // connection and check for schema consistency using the new structures that we just performed ALTER TABLE on.
      // Since we handled all of the added or removed columns, any failure at this point will be on structure changes we
      // cannot handle via code, and the DDL differences will be logged during the second call to schemasAreConsistent.
      if (databaseDDL == fileDDL) {
        true
      } else { // instead of just logging "false", we need to check to see if there were table DDL changes executed
        if (newColumnAdded) {
          // check the schema comparison again, but now with the new table structure following ALTER statements
          schemasAreConsistent(fileDataFrame, jdbcSchemaName, tableName, schemaFingerprint, url, user, pswd, spark, jdbcWriteType)
        }
        else { //if there were not any ALTER statements to execute, just fail as normal and log message
          val logMsg = (s"""
                           |
                           |
                           | $jdbcWriteType table definition for '$tableName' does not match parquet fingerprint '$schemaFingerprint'.  Bypassing updates for fingerprint $schemaFingerprint.
                           |
                           | $tableName $jdbcWriteType DB Table Schema:
                           | ${"-" * (tableName.length + jdbcWriteType.toString.length + 18)}
                           | ${databaseDDL.stripPrefix(s"CREATE TABLE $tableName (").stripSuffix(")")}
                           |
                           | $tableName Parquet Schema for Fingerprint $schemaFingerprint:
                           | ${"-" * (tableName.length + schemaFingerprint.length + 33)}
                           | ${fileDDL.stripPrefix(s"CREATE TABLE $tableName (").stripSuffix(")")}
                           |""")
          log.warn(logMsg)
          log.warn(s"Database Table Schema Definition: $tableSchemaDef")
          log.warn(s"File Schema Definition: $fileSchemaDef")
          false
        }
      }
    }
    else {
      true
    }
  }

  /** Write RAW data to a JDBC target database.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def writeJdbcRaw(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch, connection: Connection): Unit = {

    val tableName = clientConfig.jdbcConnectionRaw.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName

    log.info(s"*** Writing '${tableDataFrameWrapperForMicroBatch.tableName}' raw data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")

    val insertDF = tableDataFrameWrapperForMicroBatch.dataFrame.persist(StorageLevel.MEMORY_AND_DISK)

    // Determine if we need to create the table by checking if the table already exists.
    val url = clientConfig.jdbcConnectionRaw.jdbcUrl
    val dbm = connection.getMetaData
    val dbProductName = dbm.getDatabaseProductName

    val tableNameCaseSensitive = dbProductName match {
      case "Microsoft SQL Server" | "PostgreSQL" => tableNameNoSchema
      case "Oracle"                              => tableNameNoSchema.toUpperCase
      case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val tables = dbm.getTables(connection.getCatalog, clientConfig.jdbcConnectionRaw.jdbcSchema, tableNameCaseSensitive, Array("TABLE"))
    val tableExists = tables.next()

    // Get some data we will need for later.
    val dialect = JdbcDialects.get(url)
    val insertSchema = insertDF.schema

    // Create the table if it does not already exist.
    if (!tableExists) {
      // Build create table statement.
      val createTableDDL = getTableCreateDDL(dialect, insertSchema, tableName, JdbcWriteType.Raw, dbProductName)
      // Execute the table create DDL
      val stmt = connection.createStatement
      log.info(s"Raw - $createTableDDL")
      stmt.execute(createTableDDL)
      stmt.close()
      // Create table indexes for the new table.
      createIndexes(connection, url, tableName, JdbcWriteType.Raw)
      connection.commit()
    }

    // Build the insert statement.
    val columns = insertSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val placeholders = insertSchema.fields.map(_ => "?").mkString(",")
    val insertStatement = s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    log.info(s"Raw - $insertStatement")

    // Prepare and execute one insert statement per row in our insert dataframe.
    updateDataframe(connection, tableName, tableDataFrameWrapperForMicroBatch, insertDF, insertSchema, insertStatement, batchSize, dialect, JdbcWriteType.Raw, StatementType.RAW)
    insertDF.unpersist()
    log.info(s"*** Finished writing '${tableDataFrameWrapperForMicroBatch.tableName}' raw data data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionRaw.jdbcUrl}")
  }

  /** Merge the raw transactions into a JDBC target database applying the inserts/updates/deletes
   * according to transactions in the raw CDC data.
   *
   * @param tableDataFrameWrapperForMicroBatch has the data to be written
   */
  def writeJdbcMerged(tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch, connection: Connection): MicroBatchUpdateResult = {

    log.debug(s"+++ Merging '${tableDataFrameWrapperForMicroBatch.tableName}' data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint} as JDBC to ${clientConfig.jdbcConnectionMerged.jdbcUrl}")

    val tableName = clientConfig.jdbcConnectionMerged.jdbcSchema + "." + tableDataFrameWrapperForMicroBatch.tableName
    val tableNameNoSchema = tableDataFrameWrapperForMicroBatch.tableName
    val persistedTableDataframe = tableDataFrameWrapperForMicroBatch.dataFrame.persist(StorageLevel.MEMORY_AND_DISK)

    // Get list of CDA internal use columns to get rid of - including both the gwcbi___ and client application-created gwcdac__ columns
    val dropList = persistedTableDataframe.columns.filter(colName => !colName.toLowerCase.equals("gwcbi___seqval_hex") && (colName.toLowerCase.startsWith("gwcbi___") || colName.toLowerCase.startsWith("gwcdac__")))

    // Log total rows to be merged for this fingerprint.
    val totalCount = persistedTableDataframe.count()
    cdaMergedJDBCMetricsSource.timestamp_record_count_history.update(totalCount)
    log.debug(s"Merged - $tableName total count for all ins/upd/del: ${totalCount.toString}")

    // Filter for records to insert and drop unwanted columns.
    val insertDF = persistedTableDataframe
      .filter(col("gwcbi___operation").isin(2, 0))
      .drop(dropList: _*)
//      .persist(StorageLevel.MEMORY_AND_DISK)

    // Log total rows to be inserted for this fingerprint.
    val insertCount = insertDF.count()
    log.debug(s"Merged - $tableName insert count after filter: ${insertCount.toString}")

    // Determine if we need to create the table by checking if the table already exists.
    val url = clientConfig.jdbcConnectionMerged.jdbcUrl
    val dbm = connection.getMetaData
    val dbProductName = dbm.getDatabaseProductName
    val tableNameCaseSensitive = dbProductName match {
      case "Microsoft SQL Server" => tableNameNoSchema
      case "PostgreSQL"           => tableNameNoSchema
      case "Oracle"               => tableNameNoSchema.toUpperCase
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val tables = dbm.getTables(connection.getCatalog, clientConfig.jdbcConnectionMerged.jdbcSchema, tableNameCaseSensitive, Array("TABLE"))
    val tableExists = tables.next

    // Get some data we will need for later.
    val dialect = JdbcDialects.get(url)
    val insertSchema = insertDF.schema

    // Create the table if it does not already exist.
    if (!tableExists) {
      // Build create table statement.
      val createTableDDL = getTableCreateDDL(dialect, insertSchema, tableName, JdbcWriteType.Merged, dbProductName)
      log.info(s"Merged - $createTableDDL")
      // Execute the table create DDL
      val stmt = connection.createStatement
      stmt.execute(createTableDDL)
      stmt.close()
      cdaMergedJDBCMetricsSource.create_table_counter.inc()

      // Create table indexes for the new table.
      createIndexes(connection, url, tableName, JdbcWriteType.Merged)
      connection.commit()
    }

    // Build the insert statement.
    val columns = insertSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val insertStatement = if (clientConfig.jdbcConnectionMerged.ignoreInsertIfAlreadyExists) {
      val sourceColumns = insertSchema.fields.map(x => s"? AS ${dialect.quoteIdentifier(x.name)}").mkString(",")
      val valueColumns = insertSchema.fields.map(x => s"SOURCE.${dialect.quoteIdentifier(x.name)}").mkString(",")
      s"MERGE INTO $tableName AS TARGET USING (SELECT $sourceColumns) AS SOURCE on(TARGET.id = SOURCE.id) WHEN NOT MATCHED BY TARGET THEN INSERT ($columns) VALUES ($valueColumns);"
    } else {
      val placeholders = insertSchema.fields.map(_ => "?").mkString(",")
      s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    }
    log.debug(s"Merged - $insertStatement")

    // Prepare and execute one insert statement per row in our insert dataframe.
    val insertResult = updateDataframe(connection, tableName, tableDataFrameWrapperForMicroBatch, insertDF, insertSchema, insertStatement, batchSize, dialect, JdbcWriteType.Merged, StatementType.INSERT)
    cdaMergedJDBCMetricsSource.insert_statement_counter.inc(insertCount)
    cdaMergedJDBCMetricsSource.rows_inserted_counter.inc(insertResult.updatedRowCount)

    //    insertDF.unpersist()

    // Filter for records to update.
    val updateDF = persistedTableDataframe
      .filter(col("gwcbi___operation").isin(4))
      .drop(dropList: _*)
//      .persist(StorageLevel.MEMORY_AND_DISK)

    // Log total rows marked as updates.
    val updateCount = updateDF.count()
    log.debug(s"Merged - $tableName update count after filter: ${updateCount.toString}")

    // Generate and apply update statements based on the latest transaction for each id.
    val updateResult = if (updateCount > 0) {
      // Get the list of columns

      // Build the sql Update statement to be used as a prepared statement for the Updates.
      val colListForSetClause = updateDF.columns
        .filter(_ != "id")

      val colNamesForSetClause = colListForSetClause.map("\"" + _ + "\" = ?").mkString(", ")
      val updateStatement = s"""UPDATE $tableName SET $colNamesForSetClause WHERE "id" = ? AND "gwcbi___seqval_hex" < ? """
      log.debug(s"Merged - $updateStatement")

      // Get schema info required for updatePartition call.
      val updateSchema = updateDF.schema

      // Prepare and execute one update statement per row in our update dataframe.
      val result = updateDataframe(connection, tableName, tableDataFrameWrapperForMicroBatch, updateDF, updateSchema, updateStatement, batchSize, dialect, JdbcWriteType.Merged, StatementType.UPDATE)
      cdaMergedJDBCMetricsSource.update_statement_counter.inc(updateCount)
      cdaMergedJDBCMetricsSource.rows_updated_counter.inc(result.updatedRowCount)
      result
    } else {
      JdbcUpdateResult(0L, 0L)
    }
//    updateDF.unpersist()

    // Filter for records to be deleted.
    // Deletes should be relatively rare since most data is retired in InsuranceSuite rather than deleted.
    val deleteDF = persistedTableDataframe
      .filter(col("gwcbi___operation").isin(1))
      .selectExpr("id")
//      .persist(StorageLevel.MEMORY_AND_DISK)

    // Log number of records to be deleted.
    val deleteCount = deleteDF.count()
    log.debug(s"Merged - $tableName delete count after filter: ${deleteCount.toString}")

    // Generate and apply delete statements.
    val deleteResult = if (deleteCount > 0) {
      val deleteSchema = deleteDF.schema
      // Build the sql Delete statement to be used as a prepared statement for the Updates.
      val deleteStatement = s"""DELETE FROM $tableName WHERE "id" = ?"""
      //      val deleteStatement = "DELETE FROM " + tableName + " WHERE \"id\" = ?"
      log.debug(s"Merged - $deleteStatement")

      // Prepare and execute one delete statement per row in our delete dataframe.
      val result = updateDataframe(connection, tableName, tableDataFrameWrapperForMicroBatch, deleteDF, deleteSchema, deleteStatement, batchSize, dialect, JdbcWriteType.Merged, StatementType.DELETE)
      cdaMergedJDBCMetricsSource.delete_statement_counter.inc(deleteCount)
      cdaMergedJDBCMetricsSource.rows_deleted_counter.inc(result.updatedRowCount)
//      deleteDF.unpersist()
      result
    } else {
      JdbcUpdateResult(0L, 0L)
    }
    persistedTableDataframe.unpersist()
    log.info(s"+++ Finished writing '${tableDataFrameWrapperForMicroBatch.tableName}' data for fingerprint ${tableDataFrameWrapperForMicroBatch.schemaFingerprint}, timestamp ${tableDataFrameWrapperForMicroBatch.folderTimestamp} as merged JDBC with ${insertCount.toString} inserts, ${updateCount.toString} updates, ${deleteCount.toString} deletes, total count for all ins/upd/del: ${totalCount.toString}")
    var batchMetricsMismatch = 0L
    if (tableDataFrameWrapperForMicroBatch.batchMetricsExpectedUpdates > 0) {
      val dbUpdatedRowCount = insertResult.updatedRowCount + updateResult.updatedRowCount + deleteResult.updatedRowCount
      val dbUpdateStatementCount = insertResult.updateStatementCount + updateResult.updateStatementCount + deleteResult.updateStatementCount
      batchMetricsMismatch = tableDataFrameWrapperForMicroBatch.batchMetricsExpectedUpdates - dbUpdatedRowCount
      cdaMergedJDBCMetricsSource.batch_metrics_mismatched_rows.update(batchMetricsMismatch)
      if (clientConfig.metricsSettings.updateMismatchWarningsEnabled && batchMetricsMismatch != 0) {
        log.warn(s"Expected update count from batch-metrics is ${tableDataFrameWrapperForMicroBatch.batchMetricsExpectedUpdates}, does not match updated database row count ${dbUpdatedRowCount} for table: ${tableDataFrameWrapperForMicroBatch.tableName}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}")
      }
      if (clientConfig.metricsSettings.updateMismatchWarningsEnabled && tableDataFrameWrapperForMicroBatch.batchMetricsExpectedUpdates != dbUpdateStatementCount) {
        log.warn(s"Expected update count from batch-metrics is ${tableDataFrameWrapperForMicroBatch.batchMetricsExpectedUpdates}, does not match generated statement count ${dbUpdateStatementCount} for table: ${tableDataFrameWrapperForMicroBatch.tableName}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}")
      }
    }
    if (insertResult.updatedRowCount != insertResult.updateStatementCount) {
      cdaMergedJDBCMetricsSource.insert_affect_row_mismatch_history.update(insertResult.updateStatementCount - insertResult.updatedRowCount)
      if (clientConfig.metricsSettings.updateMismatchWarningsEnabled) {
        log.warn(s"Updated row count ${insertResult.updatedRowCount} does not match insert statement count ${insertResult.updateStatementCount} for table: ${tableDataFrameWrapperForMicroBatch.tableName}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}")
      }
    }
    if (updateResult.updatedRowCount != updateResult.updateStatementCount) {
      cdaMergedJDBCMetricsSource.update_affect_row_mismatch_history.update(updateResult.updateStatementCount - updateResult.updatedRowCount)
      if (clientConfig.metricsSettings.updateMismatchWarningsEnabled) {
        log.warn(s"Updated row count ${updateResult.updatedRowCount} does not match update statement count ${updateResult.updateStatementCount} for table: ${tableDataFrameWrapperForMicroBatch.tableName}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}")
      }
    }
    if (deleteResult.updatedRowCount != deleteResult.updateStatementCount) {
      cdaMergedJDBCMetricsSource.delete_affect_row_mismatch_history.update(deleteResult.updateStatementCount - deleteResult.updatedRowCount)
      if (clientConfig.metricsSettings.updateMismatchWarningsEnabled) {
        log.warn(s"Updated row count ${deleteResult.updatedRowCount} does not match delete statement count ${deleteResult.updateStatementCount} for table: ${tableDataFrameWrapperForMicroBatch.tableName}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}")
      }
    }
    MicroBatchUpdateResult(insertResult, updateResult, deleteResult)
  }

  protected def validateDBConnection(jdbcUrl: String, jdbcUsername: String, jdbcPassword: String): Unit = {
    val possibleConnection = Try(DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword))
    possibleConnection match {
      case Success(connection) => connection.close()
      case Failure(e)          => throw InvalidConfigParameterException("Connection could not be established. Check DB connection url and credentials.", e)
    }
  }

  def tableExists(tableName: String, jdbcSchemaName: String, url: String, user: String, pswd: String): Boolean = {
    val connection = DriverManager.getConnection(url, user, pswd)
    val dbm = connection.getMetaData
    val dbProductName = dbm.getDatabaseProductName
    val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
    val tableNameCaseSensitive = dbProductName match {
      case "Microsoft SQL Server" | "PostgreSQL" => tableNameNoSchema
      case "Oracle"                              => tableNameNoSchema.toUpperCase
      case _                                     => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    val tables = dbm.getTables(connection.getCatalog, jdbcSchemaName, tableNameCaseSensitive, Array("TABLE"))

    if (tables.next) {
      connection.close()
      true
    } else {
      connection.close()
      false
    }
  }

  /** Build and return a table create DDL statement based on the given schema definition.
   *
   * @param dialect
   * @param schema
   * @param tableName
   * @param jdbcWriteType
   * @param dbProductName
   * @return Table create DDL statement for the given table.
   */
  def getTableCreateDDL(dialect: JdbcDialect, schema: StructType, tableName: String, jdbcWriteType: JdbcWriteType.Value, dbProductName: String): String = {
    val allTableColumnsDefinitions = new StringBuilder()

    // Define specific columns we want to set as NOT NULL. Everything coming out of CDA parquet files is defined as nullable so
    // we do this to ensure there are columns available to set as PKs and/or AKs.
    var notNullCols = List("id", "gwcbi___operation", "gwcbi___seqval_hex")
    if (jdbcWriteType == JdbcWriteType.Merged) {
      // For merged data, include publicid, retired, and typecode in list of not null columns
      // so they can be included in index or constraint definitions.
      notNullCols = notNullCols ++ List("publicid", "retired", "typecode")
    }
    // Build the list of columns in alphabetic order.
    schema.fields.sortBy(f => f.name).foreach { field =>
      val nullable = !notNullCols.contains(field.name) && field.nullable
      val columnDefinition = buildDDLColumnDefinition(dialect, dbProductName, tableName, field.name, field.dataType, nullable)
      allTableColumnsDefinitions.append(s"$columnDefinition, ")
    }
    // Remove the trailing comma.
    val colsForCreateDDL = allTableColumnsDefinitions.stripSuffix(", ")
    // Build and return the final create table statement.
    s"CREATE TABLE $tableName ($colsForCreateDDL)"
  }

  /** Build and return a column definition to be used in CREATE and ALTER DDL statements.
   *
   * @param dialect
   * @param dbProductName
   * @param tableName
   * @param fieldName
   * @param fieldDataType
   * @param fieldNullable
   * @return Column definition - COLUMN_NAME TYPE_DECLARATION NULLABLE (i.e. '"ColumnName" VARCHAR(1333) NOT NULL').
   */
  def buildDDLColumnDefinition(dialect: JdbcDialect, dbProductName: String, tableName: String, fieldName: String, fieldDataType: DataType, fieldNullable: Boolean): String = {
    val columnDefinition = new StringBuilder()

    // Explicitly set the data type for string data to avoid nvarchar(max) and varchar2 types that are potentially too long or short.
    // nvarchar(max) columns can't be indexed.  Oracle JDBC converts the string datatype to VARCHAR2(255) which is potentially too short.
    val stringDataType = dbProductName match {
      case "Microsoft SQL Server" | "PostgreSQL" => "VARCHAR(1333)"
      case "Oracle"               => "VARCHAR2(1333)"
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    // Also for string data we need to handle very large text columns that we know of to avoid truncation sql exceptions.
    val largeStringDataType = dbProductName match {
      case "Microsoft SQL Server" => "VARCHAR(max)"
      case "PostgreSQL"           => "VARCHAR"
      case "Oracle"               => "VARCHAR2(32767)" // requires MAX_STRING_SIZE Oracle parameter to be set to EXTENDED.
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }
    // Also for BLOB data we need to handle differently for different platforms.
    val blobDataType = dbProductName match {
      case "Microsoft SQL Server" => "VARBINARY(max)"
      case "PostgreSQL"           => "bytea"
      case "Oracle"               => "BLOB"
      case _                      => throw new SQLException(s"Unsupported database platform: $dbProductName")
    }

    val fieldNameQuoted = dialect.quoteIdentifier(fieldName)
    val fieldDataTypeDefinition = if (fieldDataType == StringType) {

      val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
      val currentTableColumn = (tableNameNoSchema+"."+fieldName).toLowerCase(Locale.US)
      if (configLargeTextFields.contains(currentTableColumn)) largeStringDataType
      else stringDataType
    }
    else if (fieldDataType == BinaryType) blobDataType
    else if (dbProductName == "Oracle" && fieldDataType.toString.substring(0,7)=="Decimal") {
      fieldDataType match {
        case t: DecimalType => {
          if(t.scale == 0) {
            s"NUMBER(${t.precision})"
          } else {
            s"DECIMAL(${t.precision},${t.scale})"
          }
        }
      }
    }
    else if (dbProductName == "Microsoft SQL Server" && fieldDataType == TimestampType) "DATETIME2"
    else getJdbcType(fieldDataType, dialect).databaseTypeDefinition
    val nullableQualifier = if (!fieldNullable) "NOT NULL" else ""
    columnDefinition.append(s"$fieldNameQuoted $fieldDataTypeDefinition $nullableQualifier")
    columnDefinition.toString()
  }

  /**
   * @param connection    database connection
   * @param url           used to determine db platform
   * @param tableName     name of the table without the schema prefix
   * @param jdbcWriteType indicates Raw or Merged data write type
   */
  protected def createIndexes(connection: Connection, url: String, tableName: String, jdbcWriteType: JdbcWriteType.Value): Unit = {
    val stmt = connection.createStatement
    val tableNameNoSchema = tableName.substring(tableName.indexOf(".") + 1)
    if (url.toLowerCase.contains("sqlserver") || url.toLowerCase.contains("postgresql") || url.toLowerCase.contains("oracle")) {

      // Create primary key.
      var ddlPrimaryKey = s"ALTER TABLE $tableName ADD CONSTRAINT ${tableNameNoSchema}_pk PRIMARY KEY "
      if (jdbcWriteType == JdbcWriteType.Merged) {
        ddlPrimaryKey = ddlPrimaryKey + "(\"id\")"
      }
      else {
        ddlPrimaryKey = ddlPrimaryKey + "(\"id\", \"gwcbi___seqval_hex\", \"gwcbi___operation\")"
      }
      log.info(s"$jdbcWriteType - $ddlPrimaryKey")
      stmt.execute(ddlPrimaryKey)

      // Create index for Merged data.  Raw data will not have any additional indexes since columns other than
      // the PK can be null (due to records for deletes).
      if (jdbcWriteType == JdbcWriteType.Merged) {
        var ddlIndex = s"CREATE INDEX ${tableNameNoSchema}_idx1 ON $tableName "
        if (tableNameNoSchema.startsWith("pctl_") || tableNameNoSchema.startsWith("cctl_") || tableNameNoSchema.startsWith("bctl_") || tableNameNoSchema.startsWith("abtl_")) {
          ddlIndex = ddlIndex + "(\"typecode\")"
        }
        else {
          ddlIndex = ddlIndex + "(\"publicid\")"
        }
        log.info(s"$jdbcWriteType - $ddlIndex")
        stmt.execute(ddlIndex)
      }

    } else {
      log.info(s"Unsupported database.  $url. Indexes were not created.")
      stmt.close()
      throw new SQLException(s"Unsupported database platform: $url")
    }

    stmt.close()
  }

  private def updateDataframe(conn: Connection,
                              table: String,
                              tableDataFrameWrapperForMicroBatch: DataFrameWrapperForMicroBatch,
                              df: DataFrame,
                              rddSchema: StructType,
                              updateStmt: String,
                              batchSize: Long,
                              dialect: JdbcDialect,
                              jdbcWriteType: JdbcWriteType.Value,
                              statementType: StatementType): JdbcUpdateResult = {
    var completed = false
    var totalRowCount = 0L
    var totalRowsUpdatedCount = 0L
    val dbProductName = conn.getMetaData.getDatabaseProductName

    def checkForFailedUpdate(statementHints: util.List[String], rowCount: Int, resultCounts: Array[Int]): Unit = {
      if (clientConfig.metricsSettings.logUnaffectedUpdates) {
        for (i <- 0 until rowCount) ({
          if (resultCounts.length > i && resultCounts(i) == 0 && statementHints.size() > i) {
            log.warn(s"Statement did not affect any database rows in table: ${tableDataFrameWrapperForMicroBatch.tableName}, fingerprint: ${tableDataFrameWrapperForMicroBatch.schemaFingerprint}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp} with ${statementHints.get(i)}: ${updateStmt}")
          }
        })
      }
    }

    try {
      val stmt = conn.prepareStatement(updateStmt)
      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
      // For Oracle only - map nullTypes to TINYINT for Boolean to work around Oracle JDBC driver issues
      val nullTypes = rddSchema.fields.map {
        field =>
          if (dbProductName == "Oracle" && field.dataType == BooleanType) {
            JdbcType("BYTE", java.sql.Types.TINYINT).jdbcNullType
          } else {
            getJdbcType(field.dataType, dialect).jdbcNullType
          }
      }
      val numFields = rddSchema.fields.length

      val statementHints:util.List[String] = if (clientConfig.metricsSettings.logUnaffectedUpdates) {
        new util.ArrayList[String](clientConfig.outputSettings.jdbcBatchSize.toInt)
      } else {
        Collections.emptyList()
      }
      try {
        var rowCount = 0
        var idFieldIndex: Option[Int] = Option.empty
        var seqhexFieldIndex: Option[Int] = Option.empty
        df.toLocalIterator().asScala.foreach { row =>
          var i = 0
          var columnIndex = 1
          while (i < numFields) {

            val isIdField = "id".equalsIgnoreCase(rddSchema.fields(i).name)

            if (isIdField) {
              idFieldIndex = Option(i)
            }

            if (statementType != StatementType.UPDATE || !isIdField) {
              if ("gwcbi___seqval_hex".equalsIgnoreCase(rddSchema.fields{i}.name)) {
                seqhexFieldIndex = Option(i)
              }

              if (row.isNullAt(i)) {
                stmt.setNull(columnIndex, nullTypes(i))
              } else {
                setters(i).apply(stmt, row, i, columnIndex)
              }
              columnIndex = columnIndex + 1
            }
            i = i + 1
          }

          // set criteria columns for the update statement
          if (statementType == StatementType.UPDATE) {
            idFieldIndex.foreach(idIndex =>  {
              makeSetter(conn, dialect, rddSchema.fields(idIndex).dataType).apply(stmt, row, idIndex, columnIndex)
              columnIndex = columnIndex + 1
            })
            if (seqhexFieldIndex.isEmpty) {
              stmt.setLong(columnIndex, Long.MaxValue)
            } else {
              seqhexFieldIndex.foreach(index => {
                makeSetter(conn, dialect, rddSchema.fields(index).dataType).apply(stmt, row, index, columnIndex)
              })
            }
            columnIndex = columnIndex + 1
          }
          if (clientConfig.metricsSettings.logUnaffectedUpdates) {
            val id = idFieldIndex.map(index => { if (row.isNullAt(index)) "null" else row.get(index).toString }).getOrElse("no-id")
            val hexVal = seqhexFieldIndex.map(index => { if (row.isNullAt(index)) "null" else row.get(index).toString }).getOrElse("no-seqval-hex")
            statementHints.add(s"id: ${id}, seqval-hex: ${hexVal}")
          }

          stmt.addBatch()
          rowCount += 1
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            cdaMergedJDBCMetricsSource.jdbc_batch_size_history.update(rowCount)
            val resultCounts = stmt.executeBatch()
            checkForFailedUpdate(statementHints, rowCount, resultCounts)
            totalRowsUpdatedCount += resultCounts.sum
            log.info(s"$jdbcWriteType - executeBatch - ${rowCount.toString} rows for table: ${table}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}${if (clientConfig.jdbcConnectionMerged.logStatement) s" - $updateStmt" else ""}")
            rowCount = 0
            if (clientConfig.metricsSettings.logUnaffectedUpdates) {
              statementHints.clear()
            }
          }
        }

        if (rowCount > 0) {
          cdaMergedJDBCMetricsSource.jdbc_batch_size_history.update(rowCount)
          val resultCounts = stmt.executeBatch()
          totalRowsUpdatedCount += resultCounts.sum
          checkForFailedUpdate(statementHints, rowCount, resultCounts)
          log.info(s"$jdbcWriteType - executeBatch - ${rowCount.toString} rows for table: ${table}, timestamp: ${tableDataFrameWrapperForMicroBatch.folderTimestamp}${if (clientConfig.jdbcConnectionMerged.logStatement) s" - $updateStmt" else ""}")
        }
      } finally {
        stmt.close()
      }
      completed = true
    } catch {
      case e: SQLException =>
        log.error(s"Error executing statement: '${updateStmt}' for values: [${rddSchema.fields.map(f => f.name).mkString(", ")}] with error: ${e.getMessage}")
        val cause = e.getCause
        val nextCause = e.getNextException
        if (nextCause != null && cause != nextCause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (cause == null) {
            try {
              e.initCause(nextCause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(nextCause)
            }
          } else {
            e.addSuppressed(nextCause)
          }
        }
        throw e
    } finally {
      if (!completed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through and tell the user about another problem.
        log.info(s"$jdbcWriteType - Update failed for $table - $updateStmt")
      } else {
        log.info(s"$jdbcWriteType - Total rows updated for $table: $totalRowCount rows - $updateStmt")
      }
    }
    return new JdbcUpdateResult(totalRowCount, totalRowsUpdatedCount)
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for $dt.catalogString"))
  }

  /**
   * Retrieve standard jdbc types.
   *
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The default JdbcType for this DataType
   */
  private def getCommonJDBCType(dt: DataType): Option[JdbcType] = {

    dt match {
      case IntegerType    => Some(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType       => Some(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType     => Some(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType      => Some(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType      => Some(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType       => Some(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType    => Some(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType     => Some(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType     => Some(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType  => Some(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType       => Some(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Some(JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _              => None
    }
  }

  /**
   * A `JDBCValueSetter` is responsible for setting a value from `Row` into a field for
   * `PreparedStatement`. The last argument `Int` means the index for the value to be set
   * in the SQL statement and also used for the value in `Row`.
   * private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit
   */
  private type JDBCValueSetter = (PreparedStatement, Row, Int, Int) => Unit

  private def makeSetter(
                          conn: Connection,
                          dialect: JdbcDialect,
                          dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType      =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setInt(colPos, row.getInt(dataPos))
    case LongType         =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setLong(colPos, row.getLong(dataPos))
    case DoubleType       =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setDouble(colPos, row.getDouble(dataPos))
    case FloatType        =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setFloat(colPos, row.getFloat(dataPos))
    case ShortType        =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setInt(colPos, row.getShort(dataPos))
    case ByteType         =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setInt(colPos, row.getByte(dataPos))
    case BooleanType      =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setBoolean(colPos, row.getBoolean(dataPos))
    case StringType       =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setString(colPos, row.getString(dataPos))
    case BinaryType       =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setBytes(colPos, row.getAs[Array[Byte]](dataPos))
    case TimestampType    =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        val value: java.sql.Timestamp = row.getAs[java.sql.Timestamp](dataPos)
        stmt.setTimestamp(colPos, value, Calendar.getInstance(outputWriterConfig.dataTimeZone))
    case DateType         =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        val value: java.sql.Date = row.getAs[java.sql.Date](dataPos)
        stmt.setDate(colPos, value, Calendar.getInstance(outputWriterConfig.dataTimeZone))
    case t: DecimalType   =>
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        stmt.setBigDecimal(colPos, row.getDecimal(dataPos))
    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, dataPos: Int, colPos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](dataPos).toArray)
        stmt.setArray(colPos, array)
    case _                =>
      (_: PreparedStatement, _: Row, dataPos: Int, colPos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $dataPos")
  }

}

