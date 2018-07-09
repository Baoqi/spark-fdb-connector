package com.guandata.spark.fdb

import org.scalatest._

class FdbBasicTestSpec extends FlatSpec with Matchers {

  "FdbClient" should "works fine for test table creation & deletion" in {
    val storage = new FdbStorage("__unit_test")
    val tableName = "test1"
    storage.createTable(tableName, List("text1" -> ColumnDataType.StringType, "double1" -> ColumnDataType.DoubleType), List.empty)

    val tableSchema = storage.getTableDefinition(tableName).get
    tableSchema.columnNames should equal(List(FdbInstance.sysIdColumnName, "text1", "double1"))
    tableSchema.columnTypes should equal(List(ColumnDataType.UUIDType, ColumnDataType.StringType, ColumnDataType.DoubleType))
    tableSchema.primaryKeys should equal(List(FdbInstance.sysIdColumnName))

    storage.dropTable(tableName)
  }


  "FdbClient" should "works fine for basic insertion" in {
    val storage = new FdbStorage("__unit_test")
    val tableName = "test1"
    storage.createTable(tableName, List(
      "text1" -> ColumnDataType.StringType,
      "double1" -> ColumnDataType.DoubleType,
      "text2" -> ColumnDataType.StringType,
      "double2" -> ColumnDataType.DoubleType
    ), List("text1"))


    storage.insertRows(tableName,
      List("text1", "double2"),
      List(
        List("row1", Double.box(1.2345))
      ),
      false
    )

    storage.insertRows(tableName,
      List("double2", "text1"),
      List(
        List(Double.box(12.3456), "row1")
      ),
      false
    )

    storage.insertRows(tableName,
      List("text1", "text2", "double2", "double1"),
      List(
        List("row2", "row2_cell2", Double.box(123.45), Double.box(5555.45))
      ),
      false
    )

    val previewContent = storage.preview(tableName, 30)
    System.out.println(previewContent.toString)

    storage.dropTable(tableName)
  }

}
