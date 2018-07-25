package com.guandata.spark.fdb

import java.time.{Instant, LocalDate}

import com.apple.foundationdb.directory.DirectorySubspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{KeySelector, KeyValue, Range}

import scala.collection.JavaConverters._

object FdbBufferedReader {
  val BATCH_ROW_COUNT: Int = 90    //  key max size: 10,000, value max size 100,000,  transaction max size: 10,000,000 bytes, so, 10000000/110000 = 90.9

  def unpackKeyValue(dataDir: DirectorySubspace, tableDefinition: TableDefinition, kv: KeyValue): Vector[AnyRef] = {
    (dataDir.unpack(kv.getKey).getItems.asScala ++ Tuple.fromBytes(kv.getValue).getItems.asScala.drop(1)).zip(
      tableDefinition.columnTypes
    ).map{
      case (v, colType) if colType == ColumnDataType.DateType && v != null =>
        java.sql.Date.valueOf(LocalDate.ofEpochDay(v.asInstanceOf[java.lang.Number].longValue()))
      case (v, colType) if colType == ColumnDataType.TimestampType && v != null =>
        java.sql.Timestamp.from(Instant.ofEpochMilli(v.asInstanceOf[java.lang.Number].longValue()))
      case (v, colType) if colType == ColumnDataType.IntegerType && v != null =>
        Int.box(v.asInstanceOf[java.lang.Number].intValue())
      case (v, colType) if colType == ColumnDataType.ShortType && v != null =>
        Short.box(v.asInstanceOf[java.lang.Number].shortValue())
      case (v, colType) if colType == ColumnDataType.MapType && v != null =>
        v match {
          case elems: java.util.List[AnyRef] =>
            val keyParts = elems.asScala.zipWithIndex.collect{
              case (mapV, i) if i % 2 == 0 => mapV.asInstanceOf[String]
            }
            val valueParts = elems.asScala.zipWithIndex.collect{
              case (mapV, i) if i % 2 == 1 => mapV.asInstanceOf[String]
            }
            keyParts.zip(valueParts).toMap
        }
      case (v, _) =>
        v
    }.toVector
  }
}

class FdbBufferedReader(tableDefinition: TableDefinition, storage: FdbStorage, keyRange: Range) {

  private var batchItems = storage.rangeQueryAsVector(keyRange.begin, keyRange.end, FdbBufferedReader.BATCH_ROW_COUNT)
  private var currentBatchIndex = -1
  private val endKeySelector = KeySelector.firstGreaterOrEqual(keyRange.end)
  private val dataDir = storage.openDataDir(tableDefinition.tableName)

  def next(): Boolean = {
    currentBatchIndex += 1
    if (currentBatchIndex < batchItems.size) {
      true
    } else if (batchItems.size < FdbBufferedReader.BATCH_ROW_COUNT) {
      false
    } else {
      // still need to fetch more
      val newBatchBeginKeySelector = KeySelector.firstGreaterThan(batchItems.last.getKey)
      batchItems = storage.rangeQueryAsVector(newBatchBeginKeySelector, endKeySelector, FdbBufferedReader.BATCH_ROW_COUNT)
      currentBatchIndex = 0
      batchItems.nonEmpty
    }
  }

  def get(): Vector[AnyRef] = {
    val kv = batchItems(currentBatchIndex)
    FdbBufferedReader.unpackKeyValue(dataDir, tableDefinition, kv)
  }
}
