package org.apache.spark.sql.fdb

import java.util

import com.apple.foundationdb.{KeySelector, Range}
import com.apple.foundationdb.tuple.Tuple
import com.guandata.spark.fdb.{ColumnDataType, FdbStorage, TableDefinition}
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


class FdbDataReader(tableDefinition: TableDefinition, storage: FdbStorage, keyRange: Range) extends DataReader[Row] {
  private val BATCH_ROW_COUNT = 90    //  key max size: 10,000, value max size 100,000,  transaction max size: 10,000,000 bytes, so, 10000000/110000 = 90.9

  private var batchItems = storage.rangeQueryAsVector(tableDefinition.tableName, keyRange.begin, keyRange.end, BATCH_ROW_COUNT)
  private var currentBatchIndex = -1

  override def next(): Boolean = {
    currentBatchIndex += 1
    if (currentBatchIndex < batchItems.size) {
      true
    } else if (batchItems.size < BATCH_ROW_COUNT) {
      false
    } else {
      // still need to fetch more
      val newBatchBegin = KeySelector.firstGreaterThan(batchItems.last.getKey).getKey
      batchItems = storage.rangeQueryAsVector(tableDefinition.tableName, newBatchBegin, keyRange.end, BATCH_ROW_COUNT)
      currentBatchIndex = 0
      batchItems.nonEmpty
    }
  }

  override def get(): Row = {
    val kv = batchItems(currentBatchIndex)
    RowFactory.create(Tuple.fromBytes(kv.getValue).getItems.asScala.drop(1): _*)
  }

  override def close(): Unit = ()
}


class FdbDataReaderFactory(domainId: String, tableName: String, locations: Seq[String], begin: Array[Byte], end: Array[Byte]) extends DataReaderFactory[Row] {
  override def preferredLocations: Array[String] = {
    locations.toArray
  }

  override def createDataReader(): DataReader[Row] = {
    val storage = new FdbStorage(domainId)
    val tableDefinition = storage.getTableDefinition(tableName).get
    new FdbDataReader(tableDefinition, storage, new Range(begin, end))
  }
}

class FdbDataSourceReader(domainId: String, tableName: String) extends DataSourceReader {
  private val storage = new FdbStorage(domainId)
  private val tableDefinition = storage.getTableDefinition(tableName).get
  override def readSchema(): StructType = {
    import org.apache.spark.sql.types._
    StructType(tableDefinition.columnNames.zip(tableDefinition.columnTypes).map{ case (colName, colType) =>
        val sparkType = colType match {
          case ColumnDataType.LongType => LongType
          case ColumnDataType.DoubleType => DoubleType
          case ColumnDataType.FloatType => FloatType
          case ColumnDataType.StringType => StringType
          case ColumnDataType.TimestampType => TimestampType
          case ColumnDataType.DateType => DateType
          case ColumnDataType.UUIDType => StringType
        }
        StructField(name = colName, dataType = sparkType)
    }.toArray)
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val localityInfos = storage.getLocalityInfo(tableName)
    localityInfos.map{ case (locations, range) =>
      new FdbDataReaderFactory(domainId = domainId,
        tableName = tableName,
        locations = locations,
        begin = range.begin,
        end = range.end).asInstanceOf[DataReaderFactory[Row]]
    }.asJava
  }
}

class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val domainId = options.get("domain").get()
    val tableName = options.get("table").get()
    new FdbDataSourceReader(domainId = domainId, tableName = tableName)
  }
}
