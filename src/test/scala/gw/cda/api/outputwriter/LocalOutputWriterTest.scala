package gw.cda.api.outputwriter

import com.guidewire.cda.DataFrameWrapperForMicroBatch
import com.guidewire.cda.config.ClientConfigReader
import com.guidewire.cda.specs.CDAClientTestSpec
import gw.cda.api.utils.{ObjectMapperSupplier, UriUtils}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.io.IOException
import java.nio.file.Paths
import java.util.{Date, TimeZone}
import scala.io.Source

class LocalOutputWriterTest extends CDAClientTestSpec {

  private val tmpDirSystemProperty = System.getProperty("java.io.tmpdir") //This will be an OS specific temp dir
  private val testWriterPath = Paths.get(tmpDirSystemProperty, "cda-client-test").normalize().toAbsolutePath
  private val testSchemaFingerprint = "schemaFingerprint"
  private val testDirectory = testWriterPath.toFile
  private val clientConfig = ClientConfigReader.processConfigFile(testConfigPath)

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (testDirectory.exists()) {
      FileUtils.deleteDirectory(testDirectory)
    }
    if (!testDirectory.mkdir()) {
      throw new IllegalStateException(s"Error creating test temp data directory: ${testDirectory.toURI}")
    }
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      FileUtils.deleteDirectory(testDirectory)
    }
  }

  def makeDummyDataFrame(letters: String, numbers: Range): DataFrame = {
    val stringList = letters.map(_.toString)
    val tempPairList = numbers.zip(stringList)
    val pairRdd = sparkSession.sparkContext.parallelize(tempPairList)
    val schema = StructType(List(
      StructField("c1", IntegerType, nullable = false),
      StructField("c2", StringType, nullable = false),
      StructField("c3", StringType, nullable = false)))
    val rowRdd = pairRdd.map(r => Row(r._1, r._2, r._2))
    sparkSession.createDataFrame(rowRdd, schema)
  }

  def makeDummyDataFrameWithStructType(): DataFrame = {
    val structuredSchema = new StructType()
      .add("c1", new StructType()
        .add("c11", StringType)
        .add("c12", BinaryType))
      .add("c2", StringType)
      .add("c3", StringType)

    val structuredData = Seq(
      Row(Row("ABC", "something".getBytes("UTF-8")), "39192", "dummy"))
    val df = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(structuredData), structuredSchema)
    df
  }

  describe("Testing OutputWriter functionality") {

    val testWriter = FileBasedOutputWriter(OutputWriterConfig(testWriterPath.toUri, includeColumnNames = false, saveAsSingleFile = false, saveIntoTimestampDirectory = false, clientConfig = clientConfig, TimeZone.getDefault))
    val testWriterWithHeader = FileBasedOutputWriter(OutputWriterConfig(testWriterPath.toUri, includeColumnNames = true, saveAsSingleFile = false, saveIntoTimestampDirectory = false, clientConfig = clientConfig, TimeZone.getDefault))
    val testWriterWithTimestamp = FileBasedOutputWriter(OutputWriterConfig(testWriterPath.toUri, includeColumnNames = true, saveAsSingleFile = true, saveIntoTimestampDirectory = true, clientConfig = clientConfig, TimeZone.getDefault))

    val letters = "ABCDEFGHI"
    val numbers = 1 to 10
    val testTableName = "testTableName"
    val testTableNameWithHeader = "testHeaderTableName"
    val manifestLastSaveTimestamp = new Date().getTime.toString
    val schemaFingerprintTimestamp = new Date().getTime.toString

    describe("OutputWriter.validate") {
      it("should validate folders that can be written to and otherwise throw exceptions") {
        testWriter.validate()

        val writer2path = Paths.get("src/test/resources/nonexisting").toUri
        val writer2 = FileBasedOutputWriter(OutputWriterConfig(writer2path, includeColumnNames = false, saveAsSingleFile = false, saveIntoTimestampDirectory = false, clientConfig = clientConfig, TimeZone.getDefault))
        a[IOException] should be thrownBy writer2.validate()

        val writer3path = Paths.get(testManifestPath).toUri
        val writer3 = FileBasedOutputWriter(OutputWriterConfig(writer3path, includeColumnNames = false, saveAsSingleFile = false, saveIntoTimestampDirectory = false, clientConfig = clientConfig, TimeZone.getDefault))
        a[IOException] should be thrownBy writer3.validate()
      }
    }

    describe("OutputWriter.writeCSV") {
      it("should correctly write a DataFrame to a csv format and save it in a folder with the name of the table and schema fingerprint") {
        val testDF = makeDummyDataFrame(letters, numbers)
        val testDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, testDF, -1)
        testWriter.writeCSV(testDataFrameWrapperForMicroBatch)
        val testDFReread = sparkSession.sqlContext.read.csv(s"${testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).toAbsolutePath.toString}/*")
        // match schema of reread table so avoid checking column types (which are only strings in reread table)
        val testDFStringOnly = testDF.withColumn("c1", testDF("c1").cast(StringType))
        testDFReread.collect should contain theSameElementsAs testDFStringOnly.collect
      }

      it("should include column names on the first line of the csv output when this option is specified in the config") {
        val testDF = makeDummyDataFrame(letters, numbers)
        val testDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableNameWithHeader, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, testDF, -1)
        testWriterWithHeader.writeCSV(testDataFrameWrapperForMicroBatch)
        // make string of all the column names to compare against
        val schemaFieldNames = testDF.schema.fields.map(field => field.name).mkString(",")
        val headerFile = sparkSession.sparkContext.textFile(s"${testWriterPath.resolve(testTableNameWithHeader).resolve(testSchemaFingerprint).toAbsolutePath.toString}/*")
        // pull first line of CSV to make sure the column names were written to the first line
        headerFile.first() shouldEqual schemaFieldNames
      }

      it("should correctly write a DataFrame with StructType as string to a csv format ") {
        val testDF = makeDummyDataFrameWithStructType()
        val testDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, testDF, -1)
        testWriter.writeCSV(testDataFrameWrapperForMicroBatch)
        val testDFReread = sparkSession.sqlContext.read.csv(s"${testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).toAbsolutePath.toString}/*")
        val parsedCSVString = testDFReread.select("_c0").collect().map(_ (0)).toList.mkString
        val expectedOutput = "{\"c11\" : \"ABC\", \"c12\" : \"something\"}"
        parsedCSVString shouldEqual expectedOutput
      }
    }

    describe("OutputWriter.makeCSVPath") {
      it("should correctly create the path for a local csv given the table name and fingerprint") {
        val tableDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, null, -1)
        testWriter.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch).getPath shouldEqual UriUtils.prune(testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).toUri).getPath
      }

      it("should correctly create the path for a local csv given the table name, and have the timestamp") {
        val tableDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, null, -1)
        testWriterWithTimestamp.getPathToFolderWithCSV(tableDataFrameWrapperForMicroBatch).getPath shouldEqual UriUtils.prune(testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).resolve(manifestLastSaveTimestamp).toUri).getPath
      }
    }

    describe("OutputWriter.makeSchemaPath") {
      it("should correctly create the path for a local schema given the table name") {
        val tableDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, null, -1)
        testWriter.getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch).getPath shouldEqual UriUtils.prune(testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).resolve(testWriter.schemaFileName).toUri).getPath
      }

      it("should correctly create the path for a local schema given the table name and schema fingerprint, and have the timestamp") {
        val tableDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, null, -1)
        testWriterWithTimestamp.getPathToFileWithSchema(tableDataFrameWrapperForMicroBatch).getPath shouldEqual UriUtils.prune(testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).resolve(manifestLastSaveTimestamp).resolve(testWriterWithTimestamp.schemaFileName).toUri).getPath
      }
    }

    describe("OutputWriter.writeSchema") {
      it("should correctly write a yaml string that contains the schema of a table to a yaml file") {
        val testDF = makeDummyDataFrame(letters, numbers)
        val testDataFrameWrapperForMicroBatch = DataFrameWrapperForMicroBatch(testTableName, testSchemaFingerprint, schemaFingerprintTimestamp, manifestLastSaveTimestamp, testDF, -1)
        val testSchemaFieldsList = testDF.schema.fields.toList.map(field => {
          Map("name" -> field.name,
            "dataType" -> field.dataType.simpleString,
            "nullable" -> field.nullable)
        })

        testWriter.writeSchema(testDataFrameWrapperForMicroBatch)
        val testSchemaSource = Source.fromFile(testWriterPath.resolve(testTableName).resolve(testSchemaFingerprint).resolve(testWriter.schemaFileName).toFile)
        val testSchemaString = testSchemaSource.getLines.mkString("\n")
        testSchemaSource.close
        val testSchemaFieldsListReread = ObjectMapperSupplier.yamlMapper.readValue(testSchemaString, classOf[List[Map[String, String]]])
        testSchemaFieldsListReread shouldEqual testSchemaFieldsList
      }

    }

    describe("OutputWriter.makeSchemaString") {
      it("should correctly create a yaml string that contains the schema of a table") {
        val testDF = makeDummyDataFrame(letters, numbers)
        val testSchemaFieldsList = testDF.schema.fields.toList.map(field => {
          Map("name" -> field.name,
            "dataType" -> field.dataType.simpleString,
            "nullable" -> field.nullable)
        })

        val testSchemaString = testWriter.makeSchemaYamlString(testDF)
        val testSchemaFieldsListReread = ObjectMapperSupplier.yamlMapper.readValue(testSchemaString, classOf[List[Map[String, String]]])
        testSchemaFieldsListReread shouldEqual testSchemaFieldsList
      }

    }

  }
}
