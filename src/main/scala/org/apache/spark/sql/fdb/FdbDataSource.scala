package org.apache.spark.sql.fdb

import java.util

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.Tuple
import com.guandata.spark.fdb.{ColumnDataType, FdbStorage, TableDefinition}
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


class FdbDataReader(tableDefinition: TableDefinition, items: util.List[KeyValue]) extends DataReader[Row] {
  private val iter = items.iterator()
  override def next(): Boolean = iter.hasNext

  override def get(): Row = {
    val kv = iter.next()
    RowFactory.create(Tuple.fromBytes(kv.getValue).getItems.asScala.drop(1): _*)
  }

  override def close(): Unit = ()
}


class FdbDataReaderFactory(domainId: String, tableName: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    val storage = new FdbStorage(domainId)
    val tableDefinition = storage.getTableDefinition(tableName).get
    new FdbDataReader(tableDefinition, storage.openTableAsList(tableDefinition.tableName).join())
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
    List(new FdbDataReaderFactory(domainId = domainId, tableName = tableName).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}

class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val domainId = options.get("domain").get()
    val tableName = options.get("table").get()
    new FdbDataSourceReader(domainId = domainId, tableName = tableName)
  }
}
