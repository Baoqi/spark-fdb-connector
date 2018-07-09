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
}
