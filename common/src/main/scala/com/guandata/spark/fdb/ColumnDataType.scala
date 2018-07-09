package com.guandata.spark.fdb

sealed trait ColumnDataType {
  def value: String
}
object ColumnDataType {
  final case object LongType extends ColumnDataType {
    val value = "LongType"
  }
  final case object DoubleType extends ColumnDataType {
    val value = "DoubleType"
  }
  final case object FloatType extends ColumnDataType {
    val value = "FloatType"
  }
  final case object StringType extends ColumnDataType {
    val value = "StringType"
  }
  final case object TimestampType extends ColumnDataType {
    val value = "TimestampType"
  }
  final case object DateType extends ColumnDataType {
    val value = "DateType"
  }

  /**
    * NOTE: this is not supported by Spark yet
    */
  final case object UUIDType extends ColumnDataType {
    val value = "UUIDType"
  }

  def from(value: String) = {
    value match {
      case LongType.value => LongType
      case DoubleType.value => DoubleType
      case FloatType.value => FloatType
      case StringType.value => StringType
      case TimestampType.value => TimestampType
      case DateType.value => DateType
      case UUIDType.value => UUIDType
    }
  }
}
