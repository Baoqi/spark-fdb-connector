package com.guandata.spark.fdb

import org.scalatest._

class FdbBasicTestSpec extends FlatSpec with Matchers {

  "FdbClient" should "works fine for test table creation & deletion" in {
    val storage = new FdbStorage("__unit_test")
    val tableName = "test1"
    storage.createTable(tableName, List("text1" -> ColumnDataType.StringType, "double1" -> ColumnDataType.DoubleType), List.empty)

    val tableSchema = storage.getTableDefinition(tableName).get
    tableSchema.names should equal(List(FdbInstance.sysIdColumnName, "text1", "double1"))
    tableSchema.types should equal(List(ColumnDataType.UUIDType, ColumnDataType.StringType, ColumnDataType.DoubleType))
    tableSchema.primaryKeys should equal(List(FdbInstance.sysIdColumnName))

    storage.dropTable(tableName)
  }
}
