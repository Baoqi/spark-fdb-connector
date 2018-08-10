package com.guandata.spark.fdb

import com.apple.foundationdb.async.AsyncUtil
import com.apple.foundationdb.{KeySelector, KeyValue, LocalityUtil, Range, StreamingMode, Transaction}
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Entry point for FDB related operations with biased schema.
  *
  * @param domainId multi-tenants support, if you don't need to support multi-tenants, just pass in a Hard Code value, like "warehouse"
  */
class FdbStorage(domainId: String) {
  // private val fdb = FdbInstance.fdb
  private val instance: BaseInstance = RocksDBInstance
  private val metaDir = instance.createOrOpenSubspace(List(domainId, FdbInstance.sysTableMetaColumnName))

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
      // primary keys should be put in the beginning,  To simplify processing logic
      val existedPrimaryKeysSet = existedPrimaryKeys.toSet
      val (keyPart, otherPart) = columnNameTypes.partition{ d => existedPrimaryKeysSet.contains(d._1) }
      (keyPart ++ otherPart, existedPrimaryKeys)
    } else {
      (
        Seq(FdbInstance.sysIdColumnName -> ColumnDataType.UUIDType) ++ columnNameTypes,
        Seq(FdbInstance.sysIdColumnName)
      )
    }

    val created = instance.createTableIfNotExists(
      metaDir.pack(Tuple.from(tableName, "name")),
      List(
        metaDir.pack(Tuple.from(tableName, "name")) -> Tuple.from(checkedColumnNameTypes.map{ _._1 }: _*).pack(),
        metaDir.pack(Tuple.from(tableName, "type")) -> Tuple.from(checkedColumnNameTypes.map{ _._2.value }: _*).pack(),
        metaDir.pack(Tuple.from(tableName, "pk")) -> Tuple.from(checkedPrimaryKeys: _*).pack()
      )
    )

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
    val rangeResult = instance.getAllKeyValuesInRange(metaDir.range(Tuple.from(tableName)))
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

  def truncateTable(tableName: String): Try[Boolean] = {
    getTableDefinition(tableName).map{ _ =>
      /*fdb.run{ tr =>
        truncateTableInner(tr, tableName)
      }*/
      true
    }
  }

  private def truncateTableInner(tr: Transaction, tableName: String): Unit = {
    val dataDir = DirectoryLayer.getDefault.createOrOpen(tr, List(domainId, tableName).asJava, Array[Byte]()).join()
    tr.clear(dataDir.range(Tuple.from()))
  }

  def dropTable(tableName: String): Try[Boolean] = {
    getTableDefinition(tableName).map{ _ =>
      /*fdb.run{ tr =>
        truncateTableInner(tr, tableName)
        tr.clear(metaDir.range(Tuple.from(tableName)))
      }*/
      true
    }
  }

  private def insertRows(tableDefinition: TableDefinition, columnNames: Seq[String], insertValues: Seq[Seq[AnyRef]], enableMerge: Boolean): Try[Boolean] = {
    val writer = new FdbBufferedWriter(domainId, tableDefinition, enableMerge)
    insertValues.foreach{ row => writer.insertRow(columnNames, row) }
    writer.flush()
    Success(true)
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

  def openDataDir(tableName: String): Subspace = instance.openSubspace(List(domainId, tableName))

  def preview(tableName: String, limit: Int): Seq[Seq[AnyRef]] = {
    val dataDir = openDataDir(tableName)
    val range = dataDir.range()
    rangeQueryAsVector(range.begin, range.end, limit).map{ kv =>
      dataDir.unpack(kv.getKey()).getItems.asScala ++ Tuple.fromBytes(kv.getValue).getItems.asScala.drop(1)
    }
  }

  def rangeQueryAsVector(rangeBegin: Array[Byte], rangeEnd: Array[Byte], limit: Int): Vector[KeyValue] = {
    FdbInstance.wrapDbFunction { tr =>
      tr.getRange(rangeBegin, rangeEnd, limit, false, StreamingMode.EXACT).asList().join().asScala.toVector
    }
  }

  def rangeQueryAsVector(beginKeySelector: KeySelector, endKeySelector: KeySelector, limit: Int): Vector[KeyValue] = {
    FdbInstance.wrapDbFunction { tr =>
      tr.getRange(beginKeySelector, endKeySelector, limit, false, StreamingMode.EXACT).asList().join().asScala.toVector
    }
  }

  def getLocalityInfo(tableName: String): Seq[(List[String], Range)]  = {
    val dataDir = openDataDir(tableName)
    FdbInstance.wrapDbFunction { tr =>
      val dirRange = dataDir.range(Tuple.from())
      val closableIterator = LocalityUtil.getBoundaryKeys(tr, dirRange.begin, dirRange.end)
      var keyLocalityRanges: List[Array[Byte]] = List.empty[Array[Byte]]
      try {
        keyLocalityRanges = AsyncUtil.collectRemaining(closableIterator).join().asScala.toList
      } finally {
        closableIterator.close()
      }

      if (keyLocalityRanges.nonEmpty && !java.util.Arrays.equals(keyLocalityRanges.head, dirRange.begin)) {
        keyLocalityRanges = List(dirRange.begin) ++ keyLocalityRanges
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
