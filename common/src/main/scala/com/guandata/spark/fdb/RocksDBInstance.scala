package com.guandata.spark.fdb

import java.util.Base64

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, DBOptions, ReadOptions, RocksDB}
import FdbUtil.using
import com.apple.foundationdb
import com.apple.foundationdb.KeyValue

import scala.collection.JavaConverters._
import scala.collection.mutable

case class RocksDbWrapper(val db: RocksDB, val dbOptions: DBOptions, val columnFamilyHandle: ColumnFamilyHandle, val iteratorReadOptions: ReadOptions) extends AutoCloseable {
  override def close(): Unit = {
    columnFamilyHandle.close()
    db.close()
    dbOptions.close()
  }
}

object RocksDBInstance extends BaseInstance {

  var rocksDBWrapper: RocksDbWrapper = _
  def init(providedRocksDB: RocksDbWrapper): Unit = {
    rocksDBWrapper = providedRocksDB
    // load tables with ID
    this.synchronized {
      val targetMap = mutable.HashMap.empty[String, Long]
      using(rocksDBWrapper.db.newIterator(rocksDBWrapper.iteratorReadOptions)) { iter =>
        val range = rootSchemaDir.range()
        iter.seek(range.begin)
        while (iter.isValid()) {
          val path = rootSchemaDir.unpack(iter.key()).getItems.asScala.map{_.asInstanceOf[String]}.toList
          val tableId = Tuple.fromBytes(iter.value()).getLong(0)
          targetMap.put(getPathMapKey(path), tableId)
          iter.next()
        }
        dirPathToPrefixMap = targetMap.toMap
      }
    }
    System.out.println("Loadded RocksDB directory structure: " + dirPathToPrefixMap.toString)
  }

  var dirPathToPrefixMap = Map.empty[String, Long]
  val rootSchemaDir = new Subspace(Tuple.from(Long.box(256)))

  private def getPathMapKey(path: List[String]) = {
    Base64.getEncoder.encodeToString(Tuple.from(path: _*).pack())
  }

  private def createOrGetTableId(path: List[String]): Long = {
    val pathKey = getPathMapKey(path)
    this.synchronized{
      dirPathToPrefixMap.get(pathKey) match {
        case Some(id) => id
        case _ =>
          val idSet = dirPathToPrefixMap.values.toSet
          val maxId = if (idSet.nonEmpty) idSet.max else 256  // 256 is reserved for root schemaDir

          val targetId = if (Tuple.from(Long.box(maxId)).pack().size != 3) {
            // Can only create table if if size == 3
            Stream.from(257).filter(d => !idSet.contains(d)).head
          } else {
            maxId + 1
          }

          dirPathToPrefixMap = dirPathToPrefixMap + (pathKey -> targetId)
          rocksDBWrapper.db.put(rootSchemaDir.pack(Tuple.from(path: _*)), Tuple.from(Long.box(targetId)).pack())
          targetId
      }
    }
  }

  override def createOrOpenSubspace(path: List[String]): Subspace = {
    val tableId = createOrGetTableId(path)
    new Subspace(Tuple.from(Long.box(tableId)))
  }

  override def openSubspace(path: List[String]): Subspace = {
    this.synchronized{
      val pathKey = getPathMapKey(path)
      val tableId = dirPathToPrefixMap(pathKey)
      new Subspace(Tuple.from(Long.box(tableId)))
    }
  }

  def openRockDbWrapper(path: String): RocksDbWrapper = {
    val dbOptions = new DBOptions().setCreateIfMissing(true)
    val columnFamilyOptions = new ColumnFamilyOptions().useFixedLengthPrefixExtractor(3)
    val columnFamilyDescriptors = List( new ColumnFamilyDescriptor("default".getBytes("UTF-8"), columnFamilyOptions)).asJava
    val columnFamilyHandles =  new java.util.LinkedList[ColumnFamilyHandle]
    val db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles)

    val iteratorReadOptions: ReadOptions = new ReadOptions().setPrefixSameAsStart(true)
    new RocksDbWrapper(db, dbOptions, columnFamilyHandles.getFirst, iteratorReadOptions)
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

  override def getAllKeyValuesInRange(range: foundationdb.Range): mutable.Buffer[KeyValue] = {
    val result = mutable.ListBuffer.empty[KeyValue]
    val iter = rocksDBWrapper.db.newIterator(rocksDBWrapper.iteratorReadOptions)
    iter.seek(range.begin)
    while (iter.isValid() && ByteArrayUtil.compareUnsigned(iter.key(), range.end) < 0) {
      result.append(new KeyValue(iter.key(), iter.value()))
      iter.next()
    }
    result
  }
}