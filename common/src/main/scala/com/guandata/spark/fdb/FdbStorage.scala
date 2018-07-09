package com.guandata.spark.fdb

import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Entry point for FDB related operations with biased schema.
  *
  * @param domainId multi-tenants support, if you don't need to support multi-tenants, just pass in a Hard Code value, like "warehouse"
  */
class FdbStorage(domainId: String) {
  private val fdb = FdbInstance.fdb
  private val metaDir = DirectoryLayer.getDefault.createOrOpen(fdb, List(domainId, FdbInstance.sysTableMetaColumnName).asJava, Array[Byte]()).join()

  /**
    * Main API to create logic table in FDB
    *
    * @param tableName      table name
    * @param columnNameTypes  (column name, column type) pairs for this table
    * @param primaryKeys  (optional) the primary key column names (should exists in columnNameTypes), if primaryKeys is not specified, we will use UUID as the primary key
    */
  def createTable(tableName: String,
                  columnNameTypes: Seq[(String, ColumnDataType)],
                  primaryKeys: Seq[String]): Try[TableDefinition] = {
    val columnNameTypeMap = columnNameTypes.toMap
    val existedPrimaryKeys = primaryKeys.filter(columnNameTypeMap.contains)

    // insert into schema
    val (checkedColumnNameTypes, checkedPrimaryKeys) = if (existedPrimaryKeys.nonEmpty) {
      (columnNameTypes, existedPrimaryKeys)
    } else {
      (
        Seq(FdbInstance.sysIdColumnName -> ColumnDataType.UUIDType) ++ columnNameTypes,
        Seq(FdbInstance.sysIdColumnName)
      )
    }

    val created = FdbInstance.wrapDbFunction{ tr =>
      val existingTableRecord = tr.get(metaDir.pack(Tuple.from(tableName, "name"))).join()
      if (existingTableRecord != null) {
        false
      } else {
        tr.set(metaDir.pack(Tuple.from(tableName, "name")), Tuple.from(checkedColumnNameTypes.map{ _._1 }: _*).pack())
        tr.set(metaDir.pack(Tuple.from(tableName, "type")), Tuple.from(checkedColumnNameTypes.map{ _._2.value }: _*).pack())
        tr.set(metaDir.pack(Tuple.from(tableName, "pk")), Tuple.from(checkedPrimaryKeys: _*).pack())
        true
      }
    }

    if (created) {
      Failure(new FdbException(s"Table $tableName already exists in domain: $domainId"))
    } else {
      getTableDefinition(tableName)
    }
  }


  private def getValueAsStringList(content: Array[Byte]): List[String] = {
    Tuple.fromBytes(content).getItems.asScala.toList.map{ _.asInstanceOf[String] }
  }

  def getTableDefinition(tableName: String): Try[TableDefinition] = {
    FdbInstance.wrapDbFunction{ tr =>
      val rangeResult = tr.getRange(metaDir.range(Tuple.from(tableName))).asList().join().asScala
      if (rangeResult.isEmpty) {
        Failure(new FdbException(s"Table $tableName already exists in domain: $domainId"))
      } else {
        var names = List.empty[String]
        var types = List.empty[String]
        var primaryKeys = List.empty[String]
        rangeResult.foreach{ kv =>
          metaDir.unpack(kv.getKey).getString(1) match {
            case "name" =>
              names = getValueAsStringList(kv.getValue)
            case "type" =>
              types = getValueAsStringList(kv.getValue)
            case "pk" =>
              primaryKeys = getValueAsStringList(kv.getValue)
          }
        }
        Success(TableDefinition(
          names = names,
          types = types.map{ ColumnDataType.from },
          primaryKeys = primaryKeys
        ))
      }
    }
  }

  def dropTable(tableName: String): Try[Boolean] = {
    getTableDefinition(tableName).map{ _ =>
      fdb.run{ tr =>
        tr.clear(metaDir.range(Tuple.from(tableName)))
      }
      true
    }
  }
}
