package org.apache.spark.sql.fdb

import com.guandata.spark.fdb.{ColumnDataType, TableDefinition}

object FdbUtil {
  def convertTableDefinitionToStructType(tableDefinition: TableDefinition) = {
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
}
