package com.guandata.spark.fdb

import com.apple.foundationdb.async.AsyncUtil
import com.apple.foundationdb.{KeyValue, LocalityUtil, Range, StreamingMode, Transaction}
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

    if (!created) {
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
        Failure(new FdbException(s"Table $tableName not found in domain: $domainId"))
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
          tableName = tableName,
          columnNames = names,
          columnTypes = types.map{ ColumnDataType.from },
          primaryKeys = primaryKeys
        ))
      }
    }
  }

  def truncateTable(tableName: String): Try[Boolean] = {
    getTableDefinition(tableName).map{ _ =>
      fdb.run{ tr =>
        truncateTableInner(tr, tableName)
      }
      true
    }
  }

  private def truncateTableInner(tr: Transaction, tableName: String): Unit = {
    val dataDir = DirectoryLayer.getDefault.createOrOpen(tr, List(domainId, tableName).asJava, Array[Byte]()).join()
    tr.clear(dataDir.range(Tuple.from()))
  }

  def dropTable(tableName: String): Try[Boolean] = {
    getTableDefinition(tableName).map{ _ =>
      fdb.run{ tr =>
        truncateTableInner(tr, tableName)
        tr.clear(metaDir.range(Tuple.from(tableName)))
      }
      true
    }
  }

  private def getPrimaryKeyTupleFuncCreator(tableDefinition: TableDefinition, columnNames: Seq[String], insertValues: Seq[Seq[AnyRef]]): Try[Seq[AnyRef] => Tuple] = {
    // how to get combined primary keys
    val foundPkProviedIndecies: Seq[Int] = tableDefinition.primaryKeys.map{ pk => columnNames.indexOf(pk) }
    if (foundPkProviedIndecies.forall(_ >= 0)) {
      Success((row: Seq[AnyRef]) => {
        Tuple.from(foundPkProviedIndecies.map{index => row(index)}: _*)
      })
    } else {
      Failure(new FdbException(s"not all primary key values are provided when insert to table ${tableDefinition.tableName} in domain $domainId"))
    }
  }

  private def getRowContentFuncCreator(tableDefinition: TableDefinition, columnNames: Seq[String], insertValues: Seq[Seq[AnyRef]]): Try[Seq[AnyRef] => Array[AnyRef]] = {
    /**
      * The following code assume 2 types of indecies (starts from 0):
      *   1. providedIndex:   This is provided inside columnNames parameter
      *   2. storageIndex:  This is read out from tableDefinition
      */
    val storageName2StorageIndexMap = tableDefinition.columnNames.zipWithIndex.toMap

    val providedIndex2StorageIndexMap = columnNames.zipWithIndex.map{ case (name, i) =>
      i -> storageName2StorageIndexMap(name)
    }.toMap

    val storageColumnCount = tableDefinition.columnNames.size

    Success((row: Seq[AnyRef]) => {
      val storageRowCells = new Array[AnyRef](storageColumnCount)
      row.zipWithIndex.foreach{ case(cell, providedIndex) =>
        storageRowCells.update(providedIndex2StorageIndexMap(providedIndex), cell)
      }
      storageRowCells
    })
  }


  private def insertRows(tableDefinition: TableDefinition, columnNames: Seq[String], insertValues: Seq[Seq[AnyRef]], enableMerge: Boolean): Try[Boolean] = {
    val tableName = tableDefinition.tableName
    val dataDir = DirectoryLayer.getDefault.createOrOpen(fdb, List(domainId, tableName).asJava, Array[Byte]()).join()
    for {
      getPrimaryKeyFunc <- getPrimaryKeyTupleFuncCreator(tableDefinition, columnNames, insertValues)
      getRowContentFunc <- getRowContentFuncCreator(tableDefinition, columnNames, insertValues)
    } yield {
      fdb.run { tr =>
        insertValues.foreach { row =>
          val rowContentTuple = Tuple.from(Boolean.box(false)).addAll(getRowContentFunc(row).toList.asJava)
          tr.set(dataDir.pack(getPrimaryKeyFunc(row)), rowContentTuple.pack())
        }
      }
      true
    }
  }

  /**
    * Insert rows into selected table
    * @param tableName  table name
    * @param columnNames  to insert column names, no need to provide values/names for all columns, just provide columns need to be updated (BUT need to provide all cell contents for PRIMARY KEYS
    * @param insertValues Multiple Rows. each row contains cell values (the cell count of each row should be same with columnNames parameters
    * @param enableMerge  if enableMerge == true, then, it will read old row value out, and perform row merge before insertion (this affect some kind of performance). Otherwise it simply overwrite old values without read out
    */
  def insertRows(tableName: String, columnNames: Seq[String], insertValues: Seq[Seq[AnyRef]], enableMerge: Boolean): Try[Boolean] = {
    getTableDefinition(tableName).flatMap { tableDefinition =>
      if (tableDefinition.primaryKeys == List(FdbInstance.sysIdColumnName) && !columnNames.contains(FdbInstance.sysIdColumnName)) {
        insertRows(tableDefinition,
          Seq(FdbInstance.sysIdColumnName) ++ columnNames,
          insertValues.map{ row =>
            Seq(java.util.UUID.randomUUID()) ++ row
          },
          enableMerge
        )
      } else {
        insertRows(tableDefinition, columnNames, insertValues, enableMerge)
      }
    }
  }

  def preview(tableName: String, limit: Int): Seq[Seq[AnyRef]] = {
    val dataDir = DirectoryLayer.getDefault.open(fdb, List(domainId, tableName).asJava, Array[Byte]()).join()
    val range = dataDir.range()

    rangeQueryAsVector(tableName, range.begin, range.end, limit).map{ kv =>
      Tuple.fromBytes(kv.getValue).getItems.asScala
    }
  }

  def rangeQueryAsVector(tableName: String, rangeBegin: Array[Byte], rangeEnd: Array[Byte], limit: Int): Vector[KeyValue] = {
    FdbInstance.wrapDbFunction { tr =>
      tr.getRange(rangeBegin, rangeEnd, limit, false, StreamingMode.EXACT).asList().join().asScala.toVector
    }
  }

  def getLocalityInfo(tableName: String): Seq[(List[String], Range)]  = {
    val dataDir = DirectoryLayer.getDefault.open(fdb, List(domainId, tableName).asJava, Array[Byte]()).join()
    FdbInstance.wrapDbFunction { tr =>
      val dirRange = dataDir.range(Tuple.from())
      val closableIterator = LocalityUtil.getBoundaryKeys(tr, dirRange.begin, dirRange.end)
      var keyLocalityRanges: List[Array[Byte]] = List.empty[Array[Byte]]
      try {
        keyLocalityRanges = AsyncUtil.collectRemaining(closableIterator).join().asScala.toList
      } finally {
        closableIterator.close()
      }

      val splitRanges = if (keyLocalityRanges.nonEmpty) {
        keyLocalityRanges.zip(keyLocalityRanges.drop(1) ++ List(dirRange.end)).map{ case (start, end) =>
            new Range(start, end)
        }
      } else List(dirRange)

      splitRanges.map{ range =>
        val locations: List[String] = LocalityUtil.getAddressesForKey(tr, range.begin).join().toList
        locations -> range
      }
    }
  }
}
