package com.guidewire.cda

import gw.cda.api.utils.ObjectMapperSupplier

case class BatchMetrics(batchTimestamp: String, schemaId: String, tableName: String, numRecordsDropped: Long, numRecordsWritten: Long, numRecordsRead: Long)

object BatchMetricsReader {
  def readBatchMetrics(data: String) : BatchMetrics = ObjectMapperSupplier.jsonMapper.readValue(data, classOf[BatchMetrics])
}


