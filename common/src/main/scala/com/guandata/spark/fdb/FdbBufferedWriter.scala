package com.guandata.spark.fdb

import com.apple.foundationdb.Transaction
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Success, Try}


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
  protected val MAX_TRANSACTION_BYTE_SIZE = 10000000

  private val getRowContentFuncCreator = new memorized(tableDefinition, _getRowContentFuncCreator)

  private val dataDir = DirectoryLayer.getDefault.createOrOpen(FdbInstance.fdb, List(domainId, tableDefinition.tableName).asJava, Array[Byte]()).join()

  private val keyValueBuffer = mutable.ListBuffer.empty[(Array[Byte], Array[Byte])]
  private var keyValueBufferByteSize: Long = 0

  private def _getRowContentFuncCreator(tableDefinition: TableDefinition, columnNames: Seq[String]): Try[Seq[AnyRef] => (Array[AnyRef], Array[AnyRef])] = {
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
            case ColumnDataType.UUIDType =>
              cell match {
                case c: java.lang.String => FdbInstance.convertUUIDCompactStringToUUID(c)
                case _ => cell
              }
            case _ =>
              cell
          }
          storageRowCells.update(storageIndex, translatedCellValue)
        }
      }
      val pkColumnCount = tableDefinition.primaryKeys.size
      (storageRowCells.slice(0, pkColumnCount), storageRowCells.drop(pkColumnCount))
    })
  }

  def insertRow(columnNames: Seq[String], row: Seq[AnyRef]): Unit = {
    val (keyItems, valueItems) = getRowContentFuncCreator.getCreatedFunc(columnNames)(row)
    val k = dataDir.pack(Tuple.from(keyItems: _*))
    val v = Tuple.from(Boolean.box(false)).addAll(valueItems.toList.asJava).pack()
    if (keyValueBufferByteSize + k.size + v.size > getBatchByteSize) {
      flush()
    }

    keyValueBuffer.append(k -> v)
    keyValueBufferByteSize += k.size + v.size
  }

  def flush(): Unit = {
    FdbInstance.fdb.run { tr =>
      keyValueBuffer.foreach{ case (k, v) =>
        realAction(tr, k, v)
      }
    }
    keyValueBuffer.clear()
    keyValueBufferByteSize = 0
  }

  /**
    * The following are to be overrided
    */

  def realAction(tr: Transaction, k: Array[Byte], v: Array[Byte]): Unit = {
    tr.set(k, v)
  }

  def getBatchByteSize: Double = 0.98 * MAX_TRANSACTION_BYTE_SIZE
}


class FdbBufferedDeleter(domainId: String, tableDefinition: TableDefinition) extends FdbBufferedWriter(domainId, tableDefinition, enableMerge = false) {
  override def realAction(tr: Transaction, k: Array[Byte], v: Array[Byte]): Unit = {
    tr.clear(k, Tuple.fromBytes(k).range().end)
  }

  override def getBatchByteSize: Double = 0.01 * MAX_TRANSACTION_BYTE_SIZE
}
