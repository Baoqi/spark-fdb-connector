package com.guandata.spark.fdb

import org.scalatest._
import org.scalatest.Matchers._

class FdbBasicTestSpec extends FlatSpec with Matchers {
  "FdbClient" should "works" in {
    FdbInstance.fdb.run{ tr =>
      "abc" should equal("abc")
    }
  }
}
