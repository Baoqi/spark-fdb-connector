package com.guandata.spark.fdb

case class TableDefinition(names: List[String], types: List[ColumnDataType], primaryKeys: List[String])
