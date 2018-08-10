package com.guandata.spark.fdb

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

  private val storage = new FdbStorage(domainId)
  private val dataDir = storage.createOrOpenDataDir(tableDefinition.tableName)

  private val keyValueBuffer = mutable.ListBuffer.empty[(Array[Byte], Array[Byte])]
  private var keyValueBufferByteSize: Long = 0

  private var accumulateKeyByteSize: Long = 0
  private var accumulateValueByteSize: Long = 0
  private var maxKeyByteSize: Long = 0
  private var maxValueByteSize: Long = 0

  private def convertMapToJavaList(map: Map[AnyRef, AnyRef]) = {
    map.withFilter{ case (k, v) =>
      k != null && v != null
    }.flatMap{ case(k, v) =>
      List(k, v)
    }.toList.asJava
  }

  private def _getRowContentFuncCreator(tableDefinition: TableDefinition, columnNames: Seq[String]): Try[Seq[AnyRef] => (Array[AnyRef], Vector[AnyRef])] = {
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
                case c: java.lang.String => FdbUtil.convertUUIDCompactStringToUUID(c)
                case _ => cell
              }
            case ColumnDataType.MapType =>
              cell match {
                case c: java.util.Map[AnyRef, AnyRef] =>
                  convertMapToJavaList(c.asScala.toMap)
                case c: Map[AnyRef, AnyRef] =>
                  convertMapToJavaList(c)
              }
            case ColumnDataType.DoubleType =>
              cell match {
                case c: java.math.BigDecimal =>
                  // TODO: currently, only support treat BigDecimal as Double, this may lose precision!
                  Double.box(c.doubleValue())
                case _ =>
                  cell
              }
            case _ =>
              cell
          }
          storageRowCells.update(storageIndex, translatedCellValue)
        }
      }
      val pkColumnCount = tableDefinition.primaryKeys.size
      (storageRowCells.slice(0, pkColumnCount), storageRowCells.drop(pkColumnCount).toVector)
    })
  }

  private def mergeRow(rawNewRow: Vector[AnyRef], rawOldRow: Vector[AnyRef]) = {
    val maxLength = math.max(rawNewRow.size, rawOldRow.size)

    val newRow = if (rawNewRow.size < maxLength) { rawNewRow ++ Vector.fill[AnyRef](maxLength - rawNewRow.size)(null.asInstanceOf[AnyRef]) } else rawNewRow
    val oldRow = if (rawOldRow.size < maxLength) { rawOldRow ++ Vector.fill[AnyRef](maxLength - rawOldRow.size)(null.asInstanceOf[AnyRef]) } else rawOldRow

    newRow.indices.map{ i =>
      if (newRow(i) != null) {
        newRow(i)
      } else {
        oldRow(i)
      }
    }.toVector
  }

  def insertRow(columnNames: Seq[String], row: Seq[AnyRef]): Unit = {
    val (keyItems, rawValueItems) = getRowContentFuncCreator.getCreatedFunc(columnNames)(row)

    val keyTuple = Tuple.from(keyItems: _*)
    val k = dataDir.pack(keyTuple)
    var valueItems = rawValueItems
    if (enableMerge) {
      val foundRows = storage.rangeQueryAsVector(k, dataDir.range(keyTuple).end, FdbBufferedReader.BATCH_ROW_COUNT)
      if (foundRows.nonEmpty) {
        val existingCellValues = Tuple.fromBytes(foundRows.head.getValue).getItems.asScala.drop(1).toVector
        valueItems = mergeRow(rawNewRow = rawValueItems, rawOldRow = existingCellValues)
      }
    }
    val v = Tuple.from(Boolean.box(false)).addAll(valueItems.toList.asJava).pack()
    if (keyValueBufferByteSize + k.size + v.size > getBatchByteSize) {
      flush()
    }

    keyValueBuffer.append(k -> v)
    keyValueBufferByteSize += k.size + v.size

    accumulateKeyByteSize += k.size
    accumulateValueByteSize += v.size
    maxKeyByteSize = math.max(maxKeyByteSize, k.size)
    maxValueByteSize = math.max(maxValueByteSize, v.size)
  }

  def flush(): Unit = {
    storage.flushRows(keyValueBuffer, isForDelete)
    keyValueBuffer.clear()
    keyValueBufferByteSize = 0

    accumulateKeyByteSize = 0
    accumulateValueByteSize = 0
    maxKeyByteSize = 0
    maxValueByteSize = 0
  }

  /**
    * The following are to be overrided
    */
  def isForDelete: Boolean = false

  def getBatchByteSize: Double = 0.5 * MAX_TRANSACTION_BYTE_SIZE
}


class FdbBufferedDeleter(domainId: String, tableDefinition: TableDefinition) extends FdbBufferedWriter(domainId, tableDefinition, enableMerge = false) {
  override def isForDelete: Boolean = true

  override def getBatchByteSize: Double = 0.01 * MAX_TRANSACTION_BYTE_SIZE
}
