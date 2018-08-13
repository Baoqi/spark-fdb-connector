package com.guandata.spark.fdb

import java.util.concurrent.ConcurrentHashMap

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import org.rocksdb.WriteBatch
import FdbUtil.using
import com.apple.foundationdb
import com.apple.foundationdb.KeyValue

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RocksDBInstance extends BaseInstance {

  var rocksDBWrapper: RocksDbWrapper = null
  var dirPathToPrefixMap: ConcurrentHashMap[String, java.lang.Long] = null

  def init(providedRocksDB: RocksDbWrapper, providedMap: ConcurrentHashMap[String, java.lang.Long]): Unit = {
    rocksDBWrapper = providedRocksDB
    dirPathToPrefixMap = providedMap

    // load tables with ID
    dirPathToPrefixMap.synchronized {
      using(rocksDBWrapper.db.newIterator(rocksDBWrapper.iteratorReadOptions)) { iter =>
        val range = rootSchemaDir.range()
        iter.seek(range.begin)
        while (iter.isValid()) {
          val path = rootSchemaDir.unpack(iter.key()).getItems.asScala.map{_.asInstanceOf[String]}.toList
          val tableId = Tuple.fromBytes(iter.value()).getLong(0)
          dirPathToPrefixMap.putIfAbsent(getPathMapKey(path), tableId)
          iter.next()
        }
      }
    }
  }

  val rootSchemaDir = new Subspace(Tuple.from(Long.box(256)))

  private def getPathMapKey(path: List[String]) = {
    ByteArrayUtil.printable(Tuple.from(path: _*).pack())
  }

  private def createOrGetTableId(path: List[String]): Long = {
    val pathKey = getPathMapKey(path)
    if (dirPathToPrefixMap.containsKey(pathKey)) {
      dirPathToPrefixMap.get(pathKey)
    } else {
      dirPathToPrefixMap.synchronized{
        val idSet = dirPathToPrefixMap.values().asScala.toSet
        val maxId = if (idSet.nonEmpty) idSet.max else Long.box(256)  // 256 is reserved for root schemaDir

        val targetId = if (Tuple.from(maxId).pack().size != 3) {
          // Can only create table if if size == 3
          Stream.from(257).filter(d => !idSet.contains(Long.box(d))).head
        } else {
          maxId + 1
        }

        dirPathToPrefixMap.putIfAbsent(pathKey, targetId)

        val readOutId = dirPathToPrefixMap.get(pathKey)
        rocksDBWrapper.db.put(rootSchemaDir.pack(Tuple.from(path: _*)), Tuple.from(readOutId).pack())
        readOutId
      }
    }
  }

  private def removeTableId(path: List[String]) = {
    val pathKey = getPathMapKey(path)
    dirPathToPrefixMap.synchronized{
      if (dirPathToPrefixMap.containsKey(pathKey)) {
        rocksDBWrapper.db.delete(rootSchemaDir.pack(Tuple.from(path: _*)))
        // don't delete here, only pick up this change after process restart
        // dirPathToPrefixMap.remove(pathKey)
      }
    }
  }

  override def createOrOpenSubspace(path: List[String]): Subspace = {
    val tableId = createOrGetTableId(path)
    new Subspace(Tuple.from(Long.box(tableId)))
  }

  override def openSubspace(path: List[String]): Subspace = {
    dirPathToPrefixMap.synchronized {
      val pathKey = getPathMapKey(path)
      val tableId = dirPathToPrefixMap.get(pathKey)
      new Subspace(Tuple.from(tableId))
    }
  }

  override def createTableIfNotExists(checkKey: Array[Byte], writeValueIfNotExists: List[(Array[Byte], Array[Byte])]): Boolean = {
    this.synchronized{
      val existingTableRecord = rocksDBWrapper.db.get(checkKey)
      if (existingTableRecord != null) {
          false
      } else {
        writeValueIfNotExists.foreach{ case(k, v) =>
          rocksDBWrapper.db.put(rocksDBWrapper.columnFamilyHandle, k, v)
        }
        true
      }
    }
  }

  override def getAllKeyValuesInRange(range: foundationdb.Range): Vector[KeyValue] = {
    rangeQueryAsVector(range.begin, range.end, Int.MaxValue)
  }


  override def rangeQueryAsVector(rangeBegin: Array[Byte], rangeEnd: Array[Byte], limit: Int): Vector[KeyValue] = {
    val result = mutable.ListBuffer.empty[KeyValue]
    var currentCount: Long = 0
    using(rocksDBWrapper.db.newIterator(rocksDBWrapper.iteratorReadOptions)) { iter =>
      iter.seek(rangeBegin)
      while (iter.isValid() && currentCount < limit && ByteArrayUtil.compareUnsigned(iter.key(), rangeEnd) < 0) {
        result.append(new KeyValue(iter.key(), iter.value()))
        currentCount = currentCount + 1
        iter.next()
      }
    }
    result.toVector
  }


  override def truncateTable(domainId: String, tableName: String): Unit = {
    val dataDir = createOrOpenSubspace(List(domainId, tableName))
    val range = dataDir.range(Tuple.from())
    rocksDBWrapper.db.deleteRange(rocksDBWrapper.columnFamilyHandle, range.begin, range.end)
  }

  override def dropTable(domainId: String, tableName: String, metaRange: foundationdb.Range): Unit = {
    truncateTable(domainId, tableName)
    rocksDBWrapper.db.deleteRange(rocksDBWrapper.columnFamilyHandle, metaRange.begin, metaRange.end)
    removeTableId(List(domainId, tableName))
  }

  override def flushRows(rows: ListBuffer[(Array[Byte], Array[Byte])], isToDelete: Boolean): Unit = {
    using(new WriteBatch()) { batch =>
      rows.foreach{ case(k, v) =>
        if (isToDelete) {
          batch.deleteRange(rocksDBWrapper.columnFamilyHandle, k, Tuple.fromBytes(k).range().end)
        } else {
          batch.put(rocksDBWrapper.columnFamilyHandle, k, v)
        }
      }
      rocksDBWrapper.db.write(rocksDBWrapper.batchWriteOptions, batch)
    }
  }
}
