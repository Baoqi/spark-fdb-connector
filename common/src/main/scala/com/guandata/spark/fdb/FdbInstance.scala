package com.guandata.spark.fdb

import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{Database, FDB, KeyValue, Range, Transaction}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait BaseInstance {
  def createOrOpenSubspace(path: List[String]): Subspace
  def openSubspace(path: List[String]): Subspace

  def createTableIfNotExists(checkKey: Array[Byte],
                             writeValueIfNotExists: List[(Array[Byte], Array[Byte])]): Boolean

  def getAllKeyValuesInRange(range: Range): mutable.Buffer[KeyValue]

  def truncateTable(domainId: String, tableName: String): Unit
  def dropTable(domainId: String, tableName: String, metaRange: Range): Unit
}

object FdbInstance extends BaseInstance {
  lazy val fdb: Database = {
    val instance = if (!FDB.isAPIVersionSelected) {
      FDB.selectAPIVersion(520)
    } else {
      FDB.instance()
    }
    instance.open
  }

  val sysTableMetaColumnName = "__table_meta__"
  val sysIdColumnName = "__sys_id"

  def wrapDbFunction[T](func: Transaction => T): T = {
    var result: T = null.asInstanceOf[T]
    fdb.run{ tr =>
      result = func(tr)
    }
    result
  }

  override def createOrOpenSubspace(path: List[String]): Subspace = {
    DirectoryLayer.getDefault.createOrOpen(fdb, path.asJava, Array[Byte]()).join()
  }

  def openSubspace(path: List[String]): Subspace = {
    DirectoryLayer.getDefault.open(fdb, path.asJava, Array[Byte]()).join()
  }

  override def createTableIfNotExists(checkKey: Array[Byte],
                                      writeValueIfNotExists: List[(Array[Byte], Array[Byte])]): Boolean = {
    FdbInstance.wrapDbFunction{ tr =>
      val existingTableRecord = tr.get(checkKey).join()
      if (existingTableRecord != null) {
        false
      } else {
        writeValueIfNotExists.foreach{ case(k, v) =>
          tr.set(k, v)
        }
        true
      }
    }
  }

  override def getAllKeyValuesInRange(range: Range): mutable.Buffer[KeyValue] = {
    FdbInstance.wrapDbFunction { tr =>
      tr.getRange(range).asList().join().asScala
    }
  }

  private def truncateTableInner(tr: Transaction, domainId: String, tableName: String): Unit = {
    val dataDir = DirectoryLayer.getDefault.createOrOpen(tr, List(domainId, tableName).asJava, Array[Byte]()).join()
    tr.clear(dataDir.range(Tuple.from()))
  }

  override def truncateTable(domainId: String, tableName: String): Unit = {
    fdb.run{ tr =>
      truncateTableInner(tr, domainId, tableName)
    }
  }

  override def dropTable(domainId: String, tableName: String, metaRange: Range): Unit = {
    fdb.run{ tr =>
      truncateTableInner(tr, domainId, tableName)
      tr.clear(metaRange)
    }
  }
}
