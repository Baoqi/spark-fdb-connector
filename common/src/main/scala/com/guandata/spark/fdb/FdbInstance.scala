package com.guandata.spark.fdb

import com.apple.foundationdb.FDB

object FdbInstance {
  lazy val fdb = {
    val instance = if (!FDB.isAPIVersionSelected()) {
      FDB.selectAPIVersion(520)
    } else {
      FDB.instance()
    }
    instance.open
  }
}
