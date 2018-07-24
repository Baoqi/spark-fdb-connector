package org.apache.spark.sql.fdb

import com.guandata.spark.fdb.{ColumnDataType, TableDefinition}

object FdbUtil {
  import org.apache.spark.sql.types._

  val StringMapType = new MapType(StringType, StringType, valueContainsNull = false)

  def convertTableDefinitionToStructType(tableDefinition: TableDefinition): StructType = {
    StructType(tableDefinition.columnNames.zip(tableDefinition.columnTypes).map{ case (colName, colType) =>
      val sparkType = colType match {
        case ColumnDataType.LongType => LongType
        case ColumnDataType.DoubleType => DoubleType
        case ColumnDataType.FloatType => FloatType
        case ColumnDataType.StringType => StringType
        case ColumnDataType.TimestampType => TimestampType
        case ColumnDataType.DateType => DateType
        case ColumnDataType.MapType => StringMapType
        case ColumnDataType.UUIDType => StringType
      }
      StructField(name = colName, dataType = sparkType)
    }.toArray)
  }

  def checkTwoTableDefinitionContainSameColumns(existing: StructType, toInsert: StructType) = {
    val existingColumns = existing.fields.sortBy{ _.name }
    val toInsertColumns = toInsert.fields.sortBy{ _.name }
    if (existingColumns.length != toInsertColumns.length) {
      false
    } else {
      existingColumns.zip(toInsertColumns).forall{
        case (existingField, toInsertField) =>
          existingField.name == toInsertField.name && existingField.dataType.acceptsType(toInsertField.dataType)
      }
    }
  }
}
