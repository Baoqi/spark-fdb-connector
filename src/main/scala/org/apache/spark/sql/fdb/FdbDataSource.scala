package org.apache.spark.sql.fdb

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType


class FdbDataReader extends DataReader[Row] {
  override def next(): Boolean = ???

  override def get(): Row = ???

  override def close(): Unit = ???
}


class FdbDataReaderFactory extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = ???
}

class FdbDataSourceReader extends DataSourceReader {
  override def readSchema(): StructType = ???

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = ???
}

class FdbDataSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = ???
}
