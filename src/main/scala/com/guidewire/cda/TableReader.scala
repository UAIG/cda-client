package com.guidewire.cda

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.guidewire.cda.ManifestReader.ManifestMap
import com.guidewire.cda.config.ClientConfig
import gw.cda.api.outputwriter._
import gw.cda.api.outputwriter.OutputWriterConfig
import gw.cda.api.utils.S3ClientSupplier
import org.apache.commons.lang.time.StopWatch
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.metrics.source.{HikariMetricsSource, MergedJDBCMetricsSource, ReaderMetricsSource}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, to_timestamp, when}

import java.net.URI
import java.nio.file.Paths
import java.time.Duration
import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._

private[cda] case class TableS3BaseLocationInfo(tableName: String,
                                                baseURI: AmazonS3URI)

private[cda] case class TableS3BaseLocationWithFingerprintsWithUnprocessedRecords(tableName: String,
                                                                                  baseURI: AmazonS3URI,
                                                                                  fingerprintsWithUnprocessedRecords: Iterable[String])

private[cda] case class TableS3LocationWithTimestampInfo(tableName: String,
                                                         schemaFingerprint: String,
                                                         timestampSubfolderURI: AmazonS3URI,
                                                         subfolderTimestamp: Long)

private[cda] case class DataFrameWrapper(tableName: String,
                                         schemaFingerprint: String,
                                         dataFrame: DataFrame)

case class DataFrameWrapperForMicroBatch(tableName: String,
                                         schemaFingerprint: String,
                                         folderTimestamp: String,
                                         manifestTimestamp: String,
                                         dataFrame: DataFrame,
                                         batchMetricsExpectedUpdates: Long)



class TableReader(clientConfig: ClientConfig) {
  private val log = LogManager.getLogger

  private[cda] val relevantInternalColumns = Set("gwcbi___seqval_hex", "gwcbi___operation")

  // https://spark.apache.org/docs/latest/configuration.html
  private[cda] val conf: SparkConf = new SparkConf()
  conf.setAppName("CdaClient")
  private val sparkMaster = clientConfig.performanceTuning.sparkMaster match {
    case "yarn"      => clientConfig.performanceTuning.sparkMaster
    case "local" | _ =>
      if (clientConfig.performanceTuning.numberOfJobsInParallelMaxCount > Runtime.getRuntime.availableProcessors) {
        s"local[${clientConfig.performanceTuning.numberOfJobsInParallelMaxCount}]"
      } else {
        "local[*]"
      }
  }
  conf.setMaster(sparkMaster)
    .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.executor.heartbeatInterval", "200000")
    .set("spark.network.timeout", "300000")

  Option(clientConfig.sparkTuning)
    .foreach(sparkTuning => {
      // By default, Spark limits the maximum result size to 1GB, which is usually too small.
      // Setting it to '0' allows for an unlimited result size.
      val maxResultSize = Option(sparkTuning.maxResultSize).getOrElse("0")
      conf.set("spark.driver.maxResultSize", maxResultSize)
      Option(sparkTuning.executorMemory).foreach(conf.set("spark.executor.memory", _))
      Option(sparkTuning.driverMemory).foreach(conf.set("spark.driver.memory", _))
    })

  if (clientConfig.metricsSettings.sinkSettings != null) {
    clientConfig.metricsSettings.sinkSettings.foreach((kv) => if (kv._1.nonEmpty) conf.set(s"spark.metrics.conf.${kv._1}", kv._2))
  }

  if (clientConfig.sparkSettings != null) {
    clientConfig.sparkSettings.foreach((kv) => if (kv._1.nonEmpty) conf.set(kv._1, kv._2))
  }

  private[cda] val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
  private[cda] val sc: SparkContext = spark.sparkContext

  sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
  //sc.setLogLevel("INFO")

  val hikariMetricsSource = new HikariMetricsSource(sc)
  val cdaReaderMetricsSource = new ReaderMetricsSource(sc)
  val cdaMergedJDBCMetricsSource = new MergedJDBCMetricsSource(sc)

  if (clientConfig.outputSettings.saveIntoJdbcMerged) {
    Option(clientConfig.jdbcConnectionMerged).foreach(_ => {MergedConnectionPool.init(clientConfig, hikariMetricsSource)})
  }

  if (SparkEnv.get != null) {
    SparkEnv.get.metricsSystem.registerSource(hikariMetricsSource)
    SparkEnv.get.metricsSystem.registerSource(cdaReaderMetricsSource)
    SparkEnv.get.metricsSystem.registerSource(cdaMergedJDBCMetricsSource)
  }

  def run(singleTableName: String = ""): Unit = {

    //Measure the total time
    val completeRunForAllTablesStopWatch = new StopWatch()
    completeRunForAllTablesStopWatch.start()

    try {
      // Get savepoints of last read--if they exist--and validate the path to write the savepoints at
      val savepointsProcessor = new SavepointsProcessor(new URI(clientConfig.savepointsLocation.uri))

      // Create OutputWriter and validate it
      val outputPath = clientConfig.outputLocation.path
      val includeColumnNames = clientConfig.outputSettings.includeColumnNames
      val saveAsSingleFile = clientConfig.outputSettings.saveAsSingleFile
      val saveIntoTimestampDirectory = clientConfig.outputSettings.saveIntoTimestampDirectory
      val outputWriterConfig = OutputWriterConfig(Paths.get(outputPath).toAbsolutePath.toUri, includeColumnNames, saveAsSingleFile, saveIntoTimestampDirectory, clientConfig)
      val outputWriter = OutputWriter(outputWriterConfig, cdaMergedJDBCMetricsSource)
      outputWriter.validate()

      // Reading manifestMap JSON from S3
      val bucketName = clientConfig.sourceLocation.bucketName
      val manifestKey = clientConfig.sourceLocation.manifestKey
      val manifestMap: ManifestMap = ManifestReader.processManifest(bucketName, manifestKey)
      var useManifestMap = manifestMap

      cdaReaderMetricsSource.manifest_table_count_history.update(manifestMap.size)

      //.-("") //TODO Make a block-list for tables configurable

      // Prepare user configs log msg
      var logMsg =
        s"""Starting Cloud Data Access Client:
           |Source bucket: ${clientConfig.sourceLocation.bucketName}
           |Manifest has ${manifestMap.size} tables: ${manifestMap.keys.mkString(", ")}
           |Writing tables to $outputPath
           |Save into JDBC Raw: ${clientConfig.outputSettings.saveIntoJdbcRaw}
           |Save into JDBC Merged: ${clientConfig.outputSettings.saveIntoJdbcMerged}
           |Export Target: ${clientConfig.outputSettings.exportTarget}
           |File Format: ${clientConfig.outputSettings.fileFormat}
           |Save as single file: $saveAsSingleFile
           |Save files into timestamp sub directory: $saveIntoTimestampDirectory
           |Including column names in csv output: $includeColumnNames}
           |""".stripMargin

      Option(clientConfig.jdbcV2Connection).foreach { jdbcV2Connection =>
        logMsg +=
          s"""
             |jdbcV2Connection.jdbcUrl: ${jdbcV2Connection.jdbcUrl}
             |jdbcV2Connection.jdbcSchema: ${jdbcV2Connection.jdbcSchema}
             |jdbcV2Connection.jdbcSaveMode: ${jdbcV2Connection.jdbcSaveMode}
             |""".stripMargin
      }

      Option(clientConfig.jdbcConnectionRaw).foreach { jdbcConnectionRaw =>
        logMsg +=
          s"""
             |jdbcConnectionRaw.jdbcUrl: ${jdbcConnectionRaw.jdbcUrl}
             |jdbcConnectionRaw.jdbcSchema: ${jdbcConnectionRaw.jdbcSchema}
             |jdbcConnectionRaw.jdbcSaveMode: ${jdbcConnectionRaw.jdbcSaveMode}
             |""".stripMargin
      }

      Option(clientConfig.jdbcConnectionMerged).foreach { jdbcConnectionMerged =>
        logMsg +=
          s"""
             |jdbcConnectionMerged.jdbcUrl: ${jdbcConnectionMerged.jdbcUrl}
             |jdbcConnectionMerged.jdbcSchema: ${jdbcConnectionMerged.jdbcSchema}
             |""".stripMargin
      }

      // Filter the manifestMap if this is run for specific tables.
      var tablesToInclude: String = Option(clientConfig.outputSettings.tablesToInclude).getOrElse("")
      val tablesToExclude: Array[String] = Option(clientConfig.outputSettings.tablesToExclude)
        .map(s => s.replaceAll(" ", "").toLowerCase(Locale.US).split(","))
        .getOrElse(Array.empty)

      //If command line argument is present, use it instead of yaml value.
      if (singleTableName.nonEmpty) {
        tablesToInclude = singleTableName
      }

      if (tablesToInclude.nonEmpty) {
        tablesToInclude = tablesToInclude.replaceAll(" ", "").toLowerCase(Locale.US)
        val tablesList = tablesToInclude.split(",")
        logMsg = logMsg + (s"""Including ONLY ${tablesList.size} table(s): $tablesToInclude""".stripMargin)
        useManifestMap = useManifestMap.filterKeys(tablesList.contains)
      }

      if (tablesToExclude.nonEmpty) {
        useManifestMap = useManifestMap.filterKeys( k => !tablesToExclude.contains(k))
      }

      // Log user configs info we prepared above
      log.info(logMsg)

      // Iterate over the manifestMap, and process each table
      log.info("Calculating all the files to fetch, based on the manifest timestamps; this might take a few seconds...")

      // Prepare copy jobs. Finding all table fingerprints to process for all tables from the manifest is done first
      val copyJobs = useManifestMap
        .map({ case (tableName, manifestEntry) =>
          // For each entry in the manifestMap, map the table name to s3 url
          val baseUri = getTableS3BaseLocationFromManifestEntry(tableName, manifestEntry)
          val tmpFingerprintsWithUnprocessedRecords = getFingerprintsWithUnprocessedRecords(tableName, manifestEntry, savepointsProcessor)
          val fingerprintsWithUnprocessedRecords: Iterable[String] = getFingerprintToProcess(clientConfig, tmpFingerprintsWithUnprocessedRecords, tableName, savepointsProcessor, baseUri)
          TableS3BaseLocationWithFingerprintsWithUnprocessedRecords(tableName, baseUri, fingerprintsWithUnprocessedRecords)
        })
        // Map each table s3 url to all corresponding timestamp subfolder urls (with new data to copy)
        .flatMap(tableBaseLocWithUnprocessedRecords => getTableS3LocationWithTimestampsAfterLastSave(tableBaseLocWithUnprocessedRecords, savepointsProcessor))
        // Keep only timestamp subfolders that fall within the range we want to read/copy
        .filter(isTableS3LocationWithTimestampSafeToCopy(_, manifestMap))
        // Put all the timestampSubfolderLocations in a map by (table name, schema fingerprint)
        .groupBy(tableInfo => (tableInfo.tableName, tableInfo.schemaFingerprint))

      // Iterate over the manifestMap, and process each table
      log.info("Starting to fetch and process all tables in the manifest")

      //Keep track of the table-fingerprint pairs as we complete them
      val totalNumberOfTableFingerprintPairs = copyJobs.size
      val completedNumberOfTableFingerprintPairs = new AtomicInteger(0)

      // Start the Copy Jobs. Process table-fingerprint pairs in parallel.
      copyJobs.par.foreach({
        case ((tableName, schemaFingerprint), _) =>
          log.info(s"Copy job is pending for '$tableName' with fingerprint '$schemaFingerprint'")
          var completed = false
          try {
            log.info(s"Copy Job is starting for '$tableName' for fingerprint '$schemaFingerprint'")
            copyTableFingerprintPairJob(tableName, schemaFingerprint, copyJobs.get((tableName, schemaFingerprint)), useManifestMap, savepointsProcessor, outputWriter)
            completed = true
          } catch {
            case e: Throwable =>
              log.error(s"Copy Job FAILED for '$tableName' for fingerprint '$schemaFingerprint': $e")
          } finally {
            if (completed) {
              log.info(s"Copy Job is complete for '$tableName' for fingerprint '$schemaFingerprint'; completed: ${completedNumberOfTableFingerprintPairs.incrementAndGet()} of $totalNumberOfTableFingerprintPairs tables")
            }
          }
      })
      log.info("All Copy Jobs have been completed")
    } finally {
      spark.close
    }

    //All done
    completeRunForAllTablesStopWatch.stop()
    log.info(s"All tables done, took ${(completeRunForAllTablesStopWatch.getTime / 1000).toString} seconds")
  }

  /**
   * Fetch the proper Fingerprint to processed based on available timestamp folders for JDBC writes.
   * Returns all available Fingerprints for file-only writes.
   *
   * @param clientConfig            clientConfig object.
   * @param fingerprintsAvailable   List of Fingerprints with unprocessed records.
   * @param tableName               tableName evaluated.
   * @param savepointsProcessor     Savepoints processor.
   * @param baseUri                 Amazon S3 URI.
   */
  private def getFingerprintToProcess(clientConfig: ClientConfig,
                                      fingerprintsAvailable: Iterable[String],
                                      tableName: String,
                                      savepointsProcessor: SavepointsProcessor,
                                      baseUri: AmazonS3URI): Iterable[String] = {
    if (clientConfig.outputSettings.exportTarget=="jdbc") {
      // Need to allow multiple fingerprints now that we're dynamically altering table definitions
      // HOWEVER we can't handle fingerprints in parallel for a given table since we're executing
      // ALTER TABLE statements against the tables as the parquet file schemas change
      // When saving to JDBC, we want to be sure we only return one fingerprint in each load to avoid schema conflict.
      val lastReadPoint = savepointsProcessor.getSavepoint(tableName)
      val numberOfFingerprintsAvailable = fingerprintsAvailable.toSeq.length
      if(numberOfFingerprintsAvailable==1) {
        fingerprintsAvailable
      } else {
        var fingerprintToReturn: Iterable[String] = None: Iterable[String]
        val firstFingerprintInList = fingerprintsAvailable.
          toSeq.get(0)
        val nextReadPointKey = if (lastReadPoint.isDefined) { s"${baseUri.getKey}$firstFingerprintInList/${lastReadPoint.get.toLong + 1}"} else { null }
        val listObjectsRequest = new ListObjectsRequest(baseUri.getBucket, s"${baseUri.getKey}$firstFingerprintInList/", nextReadPointKey, "/", null)
        val objectList = S3ClientSupplier.s3Client.listObjects(listObjectsRequest)
        val numberOfTimestampFoldersRemaining = objectList.getCommonPrefixes.size()
        if(numberOfTimestampFoldersRemaining>0) {
          fingerprintToReturn=fingerprintsAvailable.iterator.toIterable.filter(_==firstFingerprintInList)
        } else {
          val secondFingerprintInList = fingerprintsAvailable.toSeq.get(1)
          fingerprintToReturn=fingerprintsAvailable.iterator.toIterable.filter(_==secondFingerprintInList)
        }
        fingerprintToReturn
      }
    } else {
      fingerprintsAvailable
    }
  }
  /**
   * Fetch a table-fingerprint pair from S3 and write it.
   *
   * @param tableName                   Name of the table.
   * @param schemaFingerprint           Schema fingerprint for the pair.
   * @param timestampSubfolderLocations Locations of the timestamp subfolders to read from, just for one fingerprint.
   * @param manifestMap                 Manifest entries.
   * @param savepointsProcessor         Savepoints processor.
   * @param outputWriter                Writer that controls where table is written.
   */
  private def copyTableFingerprintPairJob(tableName: String, schemaFingerprint: String,
                                          timestampSubfolderLocations: Option[Iterable[TableS3LocationWithTimestampInfo]],
                                          manifestMap: ManifestMap, savepointsProcessor: SavepointsProcessor,
                                          outputWriter: OutputWriter): Unit = {

    log.info(s"Processing '$tableName' with fingerprint '$schemaFingerprint', looking for new data, on thread ${Thread.currentThread()}")

    // Get the last timestamp in the fingerprint.  We will need this when we update savepoints later on.
    val maxTimestamp = timestampSubfolderLocations.map(_.maxBy(_.subfolderTimestamp).subfolderTimestamp).getOrElse(0)
    log.info(s"maxTimestamp for table: $tableName, fingerprint: $schemaFingerprint = $maxTimestamp")

    // Read all timestamp subfolder urls, for this table, into DataFrames using Spark
    timestampSubfolderLocations
      .map(timestampSubfolderLocationsForTable => {
        // Start the StopWatch for this table
        val tableStopwatch = new StopWatch()
        tableStopwatch.start()

        var schemaCheckDone: Boolean = false
        var schemasAreConsistent: Boolean = false

        val allDataFrameWrappersForTable = timestampSubfolderLocationsForTable.toSeq.sortBy(_.subfolderTimestamp)
        log.info(s"New data found for '$tableName' with fingerprint '$schemaFingerprint', timestamps: [${allDataFrameWrappersForTable.map(d => d.subfolderTimestamp).mkString(", ")}]")
        allDataFrameWrappersForTable.foreach(tableLocation => {

          val fetchStartTime = tableStopwatch.getTime
          val dataFrameForTable = try {
            fetchDataFrameForTableTimestampSubfolder(tableLocation)
          } catch {
            case e: Exception => {
              cdaReaderMetricsSource.data_read_error_counter.inc()
              throw e
            }
          }
          val batchMetrics = if (clientConfig.metricsSettings.batchMetricsValidationEnabled) {
            try {
              fetchBatchMetricsForTableTimestampSubfolder(tableLocation)
            } catch {
              case e: Exception => {
                cdaReaderMetricsSource.batch_metrics_read_error_counter.inc()
                if (!clientConfig.metricsSettings.ignoreBatchMetricsErrors) {
                  throw e
                }
                log.warn("Error fetching batch-metrics", e)
                Option.empty
              }
            }
          } else {
            Option.empty
          }
          cdaReaderMetricsSource.timestamp_fetch_time.update(Duration.ofMillis(tableStopwatch.getTime - fetchStartTime))

          if (!schemaCheckDone) {
            schemasAreConsistent = outputWriter.schemasAreConsistent(dataFrameForTable.dataFrame, tableName, schemaFingerprint, spark) || clientConfig.outputSettings.ignoreSchemaMismatches
            schemaCheckDone = true
          }

          if (schemasAreConsistent) {
            // Write each table and its schema to the configured output type(s) (CSV, Parquet, JDBC) to the configured location
            val startWriteTime = tableStopwatch.getTime
            val manifestTimestampForTable = manifestMap(tableName).lastSuccessfulWriteTimestamp
            val tableDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(tableName, schemaFingerprint, tableLocation.subfolderTimestamp.toString, manifestTimestampForTable, dataFrameForTable.dataFrame, batchMetrics.map(_.numRecordsWritten).getOrElse({ -1L}))
            outputWriter.write(tableDataFrameWrapperForMicroBatch)
            val fullWriteTime = tableStopwatch.getTime - startWriteTime
            cdaReaderMetricsSource.timestamp_write_timer.update(Duration.ofMillis(fullWriteTime))
            // only count batch metrics for records that were written
            batchMetrics.foreach(bm => {
              cdaReaderMetricsSource.batch_metrics_dropped_counter.inc(bm.numRecordsDropped)
              cdaReaderMetricsSource.batch_metrics_written_counter.inc(bm.numRecordsWritten)
              cdaReaderMetricsSource.batch_metrics_read_counter.inc(bm.numRecordsRead)
            })
            log.info(s"Wrote timestamp ${tableLocation.subfolderTimestamp} for table '${tableName}', fingerprint '${schemaFingerprint}', took ${(fullWriteTime / 1000.0).toString} seconds")

            // If loading to jdbc target, only one Fingerprint will be processed at a time.
            // If loading files (CSV, Parquet), all Fingerprints are loaded.
            // Savepoints logic will be different for those scenarios.
            //    * If loading jdbc, use the last timestamp folder actually written.
            //    * If loading files, use the lastSuccessfulWriteTimestamp for that table from manifest.json.
            savepointsProcessor.writeSavepoints(tableName, tableLocation.subfolderTimestamp.toString)
          }
        })

        // Get a list of all fingerprints that follow the one we are processing.
        val manifestEntry = manifestMap(tableName)
        val schemaFingerprintTimestamp = manifestEntry.schemaHistory.getOrElse(schemaFingerprint, "unknown")
        val fingerprintsAfterCurrent = manifestEntry.schemaHistory
          .map({ case (schemaFingerprint, timestamp) => (schemaFingerprint, timestamp.toLong) })
          .filter({ case (_, timestamp) => timestamp > schemaFingerprintTimestamp.toLong })
          .toList
          .sortBy({ case (_, timestamp) => timestamp })

        // Log a warning message listing any additional fingerprints for this table
        // that are not being processed.
        if (clientConfig.outputSettings.exportTarget == "jdbc" && fingerprintsAfterCurrent.nonEmpty) {
          val bypassedFingerprintsList = fingerprintsAfterCurrent.map({ case (schemaFingerprint, _) => schemaFingerprint })
          log.info(
            s"""
               | $tableName fingerprint(s) were not processed in this load: ${bypassedFingerprintsList.toString.stripPrefix("List(").stripSuffix(")")}
               |   Only one fingerprint per table can be processed at a time.""")
        }


        //Stop the StopWatch, and print out the results
        tableStopwatch.stop()
        cdaReaderMetricsSource.table_process_time_history.update(Duration.ofMillis(tableStopwatch.getTime))
        log.info(s"Processed '$tableName' for fingerprint '$schemaFingerprint', took ${(tableStopwatch.getTime / 1000.0).toString} seconds")
      })
      .getOrElse(log.info(s"Skipping '$tableName' for fingerprint '$schemaFingerprint', no new data found"))
  }

  /**
   * Get the base S3 URI for a table from a manifest entry.
   *
   * @param tableName     The name of the table.
   * @param manifestEntry The manifest entry.
   * @return The base S3 URI for the table.
   */
  private[cda] def getTableS3BaseLocationFromManifestEntry(tableName: String, manifestEntry: ManifestEntry): AmazonS3URI = {
    val uriString = if (manifestEntry.dataFilesPath.endsWith("/")) {
      manifestEntry.dataFilesPath
    } else {
      manifestEntry.dataFilesPath + "/"
    }

    new AmazonS3URI(uriString)
  }

  /** Function to map a TableLocationInfo object to a list of TimestampSubfolderInfo objects.
   * "Timestamp subfolders" refer to the timestamped folders stored under the URI for each table,
   * each of which contain files for that table processed at that timestamp. Each TimestampSubfolderInfo
   * contains its corresponding table name, S3 URI, and parsed timestamp.
   *
   * @param tableInfo TableS3BaseLocationInfo containing table name and S3 URI
   * @return List[TableS3LocationWithTimestampInfo] containing timestamp subfolders' information for that table
   */
  private[cda] def getTableS3LocationWithTimestampsAfterLastSave(tableInfo: TableS3BaseLocationWithFingerprintsWithUnprocessedRecords, savepointsProcessor: SavepointsProcessor): Iterable[TableS3LocationWithTimestampInfo] = {
    val s3URI = tableInfo.baseURI
    val lastReadPoint = savepointsProcessor.getSavepoint(tableInfo.tableName)
    log.debug(s"Last read point timestamp for ${tableInfo.tableName} is $lastReadPoint")
    tableInfo.fingerprintsWithUnprocessedRecords.flatMap(fingerprint => {
      val nextReadPointKey = if (lastReadPoint.isDefined) { s"${s3URI.getKey}$fingerprint/${lastReadPoint.get.toLong + 1}"} else { null }
      val listObjectsRequest = new ListObjectsRequest(s3URI.getBucket, s"${s3URI.getKey}$fingerprint/", nextReadPointKey, "/", null)
      var objectLists = List(S3ClientSupplier.s3Client.listObjects(listObjectsRequest))
      while (objectLists.last.isTruncated) {
        objectLists = objectLists :+ S3ClientSupplier.s3Client.listNextBatchOfObjects(objectLists.last)
      }
      val timestampSubfolderKeys = objectLists.flatMap(_.getCommonPrefixes)
      timestampSubfolderKeys.map(timestampSubfolderKey => {
        val timestampSubfolderURI = new AmazonS3URI(s"s3://${s3URI.getBucket}/$timestampSubfolderKey")
        val timestampPattern = ".+\\/([0-9]+)\\/$".r
        val timestampPattern(timestamp) = timestampSubfolderKey
        TableS3LocationWithTimestampInfo(tableInfo.tableName, fingerprint, timestampSubfolderURI, timestamp.toLong)
      })
    })
  }

  /** Determine if TableS3LocationWithTimestampInfo objects fall in the time window from which we want to read.
   * Currently, this function uses the lastSuccessfulWriteTimestamp in the manifest entry per table.
   *
   * @param tableTimestampSubfolderInfo TableS3LocationWithTimestampInfo corresponding to a timestamp subfolder for some table
   * @param manifest                    Manifest data (a map of string, TableManifestData) used to look up last successful write timestamps
   * @return Boolean if this timestamp subfolder should be read/copied
   */
  private[cda] def isTableS3LocationWithTimestampSafeToCopy(tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo, manifest: ManifestMap): Boolean = {
    val manifestTimestampForTable = manifest(tableTimestampSubfolderInfo.tableName).lastSuccessfulWriteTimestamp.toLong
    val includeFolder = tableTimestampSubfolderInfo.subfolderTimestamp <= manifestTimestampForTable
    if (!includeFolder) {
      log.debug(
        s"""Filtered out timestamp subfolder with timestamp ${tableTimestampSubfolderInfo.subfolderTimestamp} for table ${tableTimestampSubfolderInfo.tableName}
           |for being later than the manifest last successful write timestamp $manifestTimestampForTable""".stripMargin.replaceAll("\n", " ")
      )
    }
    includeFolder
  }

  /** Function to read each timestamp subfolder's files into a Spark DataFrame. Results will be stored in
   * a DataFrameWrapper object that contains the read DataFrame and the table name that it corresponds to.
   * It will also drop irrelevant internal columns from the DataFrame.
   *
   * @param tableTimestampSubfolderInfo TimestampSubfolderInfo corresponding to a timestamp subfolder for some table
   * @return DataFrameWrapper object that contains the DataFrame and table name
   */
  private[cda] def fetchDataFrameForTableTimestampSubfolder(tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo): DataFrameWrapper = {
    val s3URI = tableTimestampSubfolderInfo.timestampSubfolderURI
    val s3aURL = s"s3a://${s3URI.getBucket}/${s3URI.getKey}*.parquet"
    log.info(s"Reading '${tableTimestampSubfolderInfo.tableName}' from $s3aURL, on thread ${Thread.currentThread()}")
    val dataFrame = spark.sqlContext.read.parquet(s3aURL)

    val dataFrameWrapper = manageDataFrameColumns(dataFrame, tableTimestampSubfolderInfo)
    dataFrameWrapper
  }

  /**
   * Read batch metrics for this table timestamp foler.
   *
   * @param tableTimestampSubfolderInfo TimestampSubfolderInfo corresponding to a timestamp subfolder for some table
   * @return The parsed batch-metrics
   */
  private[cda] def fetchBatchMetricsForTableTimestampSubfolder(tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo): Option[BatchMetrics] = {
    val s3URI = tableTimestampSubfolderInfo.timestampSubfolderURI
    val s3aURL = s"s3a://${s3URI.getBucket}/${s3URI.getKey}.cda/batch-metrics.json"
    log.info(s"Reading '${tableTimestampSubfolderInfo.tableName}' batch-metrics from $s3aURL, on thread ${Thread.currentThread()}")
    val dataFrame = spark.sqlContext.read.textFile(s3aURL)

    if (dataFrame.isEmpty) {
      Option.empty
    } else {
      Option.apply(BatchMetricsReader.readBatchMetrics(dataFrame.first()))
    }
  }

  /** Function that takes a DataFrame corresponding to some table and manages the addition and removal of
   * internal columns necessary for various forms of output.
   * Internal columns are currently defined as columns that begin with `gwcbi___`
   * Additional internal columns will include fingerprint and timestamp folder information prefixed with 'gwcdac__'
   *
   * @param dataFrame DataFrame with internal columns
   * @param tableTimestampSubfolderInfo TableTimestampSubfolderInfo
   * @return DataFrame that is the same as input, but has columns either added or removed
   */
  private[cda] def manageDataFrameColumns(dataFrame: DataFrame, tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo): DataFrameWrapper = {
    val dataFrameNoInternalColumns = dropIrrelevantInternalColumns(dataFrame)
    val dataFrameAdditionalColumns = addCdaClientColumns(dataFrame, tableTimestampSubfolderInfo)

    clientConfig.outputSettings.exportTarget match {
      case "file"             =>
        DataFrameWrapper(tableTimestampSubfolderInfo.tableName, tableTimestampSubfolderInfo.schemaFingerprint, dataFrameNoInternalColumns)
      case "jdbc" | "jdbc_v2" =>
        // Remove columns that will not insert properly into database tables ("spatial", "textdata")
        val dropList = dataFrameAdditionalColumns.columns.filter(colName => colName.toLowerCase.contains("spatial") || colName.toLowerCase.equals("textdata"))

        // Rename columns - column name "interval" is treated as internal reference with jdbc driver. had to rename with "_" on the end.
        val jdbcDataFrame = dataFrameAdditionalColumns
          .withColumnRenamed("interval", "interval_")
          .drop(dropList: _*)

        DataFrameWrapper(tableTimestampSubfolderInfo.tableName, tableTimestampSubfolderInfo.schemaFingerprint, jdbcDataFrame)
    }
  }

  /** Function that takes a DataFrame corresponding to some table and drops internal columns that
   * are not necessary for the client.
   * Internal columns are currently defined as columns that begin with `gwcbi___`
   *
   * @param dataFrame DataFrame with internal columns
   * @return DataFrame that is the same as input, but has irrelevant internal columns removed
   */
  private[cda] def dropIrrelevantInternalColumns(dataFrame: DataFrame): DataFrame = {
    val dropList = dataFrame.columns.filter(colName => (colName.toLowerCase.startsWith("gwcbi___") && !relevantInternalColumns.contains(colName)))
    dataFrame.drop(dropList: _*)
  }

  /** Function that adds custom metadata columns to the dataframe for troubleshooting purposes
   *
   * @param dataFrame DataFrame with internal columns
   * @param tableTimestampSubfolderInfo TableTimestampSubfolderInfo
   * @return DataFrame that is the same as input, but has irrelevant internal columns removed
   */
  private[cda] def addCdaClientColumns(dataFrame: DataFrame, tableTimestampSubfolderInfo: TableS3LocationWithTimestampInfo): DataFrame = {
    dataFrame
      .withColumn("gwcdac__fingerprintfolder", when(lit(tableTimestampSubfolderInfo.schemaFingerprint).isNotNull, lit(tableTimestampSubfolderInfo.schemaFingerprint)).otherwise(lit(null)))
      .withColumn("gwcdac__timestampfolder", when(lit(tableTimestampSubfolderInfo.subfolderTimestamp.toString).isNotNull, lit(tableTimestampSubfolderInfo.subfolderTimestamp.toString)).otherwise(lit(null)))
  }

  /** Function that takes two DataFrameWrapper objects that correspond to the same table and returns a
   * DataFrameWrapper object that contains their two DataFrames unioned together, as well as keeping the
   * name.
   *
   * @param dataFrameWrapper1 DataFrameWrapper object that has a DataFrame for a table
   * @param dataFrameWrapper2 DataFrameWrapper object that has a DataFrame for the same table
   * @return DataFrameWrapper object that contains their two dataframes unioned together and the table name.
   */
  private[cda] def reduceDataFrameWrappers(dataFrameWrapper1: DataFrameWrapper, dataFrameWrapper2: DataFrameWrapper): DataFrameWrapper = {
    var dataFrame1 = dataFrameWrapper1.dataFrame
    var dataFrame2 = dataFrameWrapper2.dataFrame

    val fieldList2 = dataFrame2.schema.fields.toList

    for (field <- dataFrame1.schema.fields) {
      val otherIndex = dataFrame2.schema.fieldIndex(field.name)
      val otherField = fieldList2.get(otherIndex)
      if (!field.dataType.equals(otherField.dataType)) {
        if ("timestamp".equals(field.dataType.typeName)) {
          dataFrame2 = dataFrame2.withColumn(field.name, to_timestamp(dataFrame2.col(field.name)))
        } else if ("timestamp".equals(otherField.dataType.typeName)) {
          dataFrame1 = dataFrame1.withColumn(field.name, to_timestamp(dataFrame1.col(field.name)))
        }
      }
    }

    val combinedDataFrame = dataFrame1.unionByName(dataFrame2)

    //Make a new instance and return it as the unionized DataFrame
    dataFrameWrapper1.copy(dataFrame = combinedDataFrame)
  }

  /**
   * Combine multiple data frames that are part of the same table and have the same schema fingerprint into one.
   *
   * @param tableName            The name of the table.
   * @param dataFrameWrapperList The data frames to coalesce.
   * @return The combined data frame.
   */
  private[cda] def reduceTimestampSubfolderDataFrames(tableName: String, dataFrameWrapperList: Iterable[DataFrameWrapper]): DataFrame = {
    log.info(s"Reducing '$tableName' with ${dataFrameWrapperList.size} data frames")
    val tableCompleteDFInfo = dataFrameWrapperList.reduce(reduceDataFrameWrappers)
    log.info(s"Reduced '$tableName' complete, now it is a single data frame")
    tableCompleteDFInfo.dataFrame
  }

  /**
   * Get fingerprints with records not yet processed.
   *
   * @param tableName           Table to fetch fingerprints for.
   * @param manifestEntry       Manifest entry corresponding to the table.
   * @param savepointsProcessor Savepoint data processor.
   * @return Fingerprints for the table with records not yet processed.
   */
  private[cda] def getFingerprintsWithUnprocessedRecords(tableName: String, manifestEntry: ManifestEntry,
                                                         savepointsProcessor: SavepointsProcessor): Iterable[String] = {
    val lastProcessedTimestamp: Long = savepointsProcessor.getSavepoint(tableName).map(_.toLong).getOrElse(-1)
    val fingerprintsSortedByTimestamp = manifestEntry.schemaHistory
      .map({ case (schemaFingerprint, timestamp) => (schemaFingerprint, timestamp.toLong) })
      .toList
      .sortBy({ case (_, timestamp) => timestamp })


    val endOfTimeline = (fingerprintsSortedByTimestamp.last._1, Long.MaxValue)
    (fingerprintsSortedByTimestamp :+ endOfTimeline)
      .sliding(2)
      // Each fingerprint marks an interval in time. The start of the interval is
      // the timestamp in the fingerprint's manifest entry, and the end is the timestamp of the next,
      // or (effectively) infinity, if no such entry exists.
      .map({ case List((fingerprint, _), (_, schemaEndTimestamp)) => (fingerprint, schemaEndTimestamp) })
      // A fingerprint interval has unprocessed entries if we are inside it or it is further
      // along in time, i.e. the endpoint is further along than where we left off.
      .filter({ case (_, schemaEndTimestamp) => schemaEndTimestamp > lastProcessedTimestamp })
      .map({ case (fingerprint, _) => fingerprint })
      .toSeq
  }

}