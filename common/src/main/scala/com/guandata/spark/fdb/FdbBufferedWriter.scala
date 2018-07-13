package com.guandata.spark.fdb

import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


private [fdb] class memorized[T](tableDefinition: TableDefinition, creatorFunc: (TableDefinition, Seq[String]) => Try[Seq[AnyRef] => T]) {
  private var privColumnNames = Seq.empty[String]
  private var createdFunc: Seq[AnyRef] => T = _

  def getCreatedFunc(columnNames: Seq[String]): Seq[AnyRef] => T = {
    if (privColumnNames eq columnNames) {
      createdFunc
    } else if (privColumnNames == columnNames) {
      createdFunc
    } else {
      privColumnNames = columnNames
      createdFunc = creatorFunc(tableDefinition, columnNames).get
      createdFunc
    }
  }
}


class FdbBufferedWriter(domainId: String, tableDefinition: TableDefinition, enableMerge: Boolean) {
  private val MAX_TRANSACTION_BYTE_SIZE = 10000000

  private val getPrimaryKeyTupleFuncCreator = new memorized(tableDefinition, _getPrimaryKeyTupleFuncCreator)
  private val getRowContentFuncCreator = new memorized(tableDefinition, _getRowContentFuncCreator)

  private val dataDir = DirectoryLayer.getDefault.createOrOpen(FdbInstance.fdb, List(domainId, tableDefinition.tableName).asJava, Array[Byte]()).join()

  private val keyValueBuffer = mutable.ListBuffer.empty[(Array[Byte], Array[Byte])]

  private def _getPrimaryKeyTupleFuncCreator(tableDefinition: TableDefinition, columnNames: Seq[String]): Try[Seq[AnyRef] => Tuple] = {
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

  private def _getRowContentFuncCreator(tableDefinition: TableDefinition, columnNames: Seq[String]): Try[Seq[AnyRef] => Array[AnyRef]] = {
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

    val storageTypes = tableDefinition.columnTypes.toVector

    Success((row: Seq[AnyRef]) => {
      val storageRowCells = new Array[AnyRef](storageColumnCount)
      row.zipWithIndex.foreach{ case(cell, providedIndex) =>
        val storageIndex = providedIndex2StorageIndexMap(providedIndex)
        if (cell == null) {
          storageRowCells.update(storageIndex, cell)
        } else {
          val translatedCellValue: AnyRef = storageTypes(storageIndex) match {
            case ColumnDataType.DateType =>
              cell match {
                case c: java.time.LocalDate => Long.box(c.toEpochDay)
                case c: java.sql.Date => Long.box(c.toLocalDate.toEpochDay)
              }
            case ColumnDataType.TimestampType =>
              cell match {
                case c: java.util.Date => Long.box(c.toInstant.toEpochMilli)
              }
            case _ =>
              cell
          }
          storageRowCells.update(storageIndex, translatedCellValue)
        }
      }
      storageRowCells
    })
  }

  def insertRow(columnNames: Seq[String], row: Seq[AnyRef]): Unit = {
    val rowContentTuple = Tuple.from(Boolean.box(false)).addAll(getRowContentFuncCreator.getCreatedFunc(columnNames)(row).toList.asJava)
    keyValueBuffer.append(
      dataDir.pack(getPrimaryKeyTupleFuncCreator.getCreatedFunc(columnNames)(row)) -> rowContentTuple.pack())
  }

  def flush(): Unit = {
    FdbInstance.fdb.run { tr =>
      keyValueBuffer.foreach{ case (k, v) =>
        tr.set(k, v)
      }
    }
  }
}