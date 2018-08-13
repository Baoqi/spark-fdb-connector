package com.guandata.spark.fdb

import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, DBOptions, ReadOptions, RocksDB, WriteBatch, WriteOptions}
import scala.collection.JavaConverters._

case class RocksDbWrapper(val db: RocksDB,
                          val dbOptions: DBOptions,
                          val columnFamilyHandle: ColumnFamilyHandle,
                          val iteratorReadOptions: ReadOptions,
                          val batchWriteOptions: WriteOptions) extends AutoCloseable {
  override def close(): Unit = {
    columnFamilyHandle.close()
    db.close()
    dbOptions.close()
    iteratorReadOptions.close()
    batchWriteOptions.close()
  }
}

object RocksDbWrapper {
  def openRockDbWrapper(path: String): RocksDbWrapper = {
    val dbOptions = new DBOptions().setCreateIfMissing(true)
    val columnFamilyOptions = new ColumnFamilyOptions().useFixedLengthPrefixExtractor(3)
    val columnFamilyDescriptors = List( new ColumnFamilyDescriptor("default".getBytes("UTF-8"), columnFamilyOptions)).asJava
    val columnFamilyHandles =  new java.util.LinkedList[ColumnFamilyHandle]
    val db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles)

    val iteratorReadOptions = new ReadOptions().setPrefixSameAsStart(true)
    val batchWriteOptions = new WriteOptions().setDisableWAL(true)
    new RocksDbWrapper(db, dbOptions, columnFamilyHandles.getFirst, iteratorReadOptions, batchWriteOptions)
  }
}