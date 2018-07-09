package com.guandata.spark.fdb

case class TableDefinition(tableName: String, columnNames: List[String], columnTypes: List[ColumnDataType], primaryKeys: List[String])
