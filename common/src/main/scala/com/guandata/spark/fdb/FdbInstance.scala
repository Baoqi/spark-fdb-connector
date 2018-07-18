package com.guandata.spark.fdb

import com.apple.foundationdb.{FDB, Transaction}

object FdbInstance {
  lazy val fdb = {
    val instance = if (!FDB.isAPIVersionSelected()) {
      FDB.selectAPIVersion(520)
    } else {
      FDB.instance()
    }
    instance.open
  }

  val sysTableMetaColumnName = "__table_meta__"
  val sysIdColumnName = "__sys_id"

  def wrapDbFunction[T](func: Transaction => T) = {
    var result: T = null.asInstanceOf[T]
    fdb.run{ tr =>
      result = func(tr)
    }
    result
  }

  def convertUUIDCompactStringToUUID(str: String) = {
    val shortUUIDCharArray = str.toCharArray
    new StringBuilder(38)
      .append(shortUUIDCharArray.subSequence(0, 8))
      .append('-')
      .append(shortUUIDCharArray.subSequence(8, 12))
      .append('-')
      .append(shortUUIDCharArray.subSequence(12, 16))
      .append('-')
      .append(shortUUIDCharArray.subSequence(16, 20))
      .append('-')
      .append(shortUUIDCharArray.subSequence(20, 32))
      .toString()
  }
}
