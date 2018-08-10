package com.guandata.spark.fdb

import java.util.UUID

object FdbUtil {
  def using[X <: {def close()}, A](resource : X)(f : X => A) = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

  def convertUUIDCompactStringToUUID(str: String): UUID = {
    val shortUUIDCharArray = str.toCharArray
    UUID.fromString(new StringBuilder(38)
      .append(shortUUIDCharArray.subSequence(0, 8))
      .append('-')
      .append(shortUUIDCharArray.subSequence(8, 12))
      .append('-')
      .append(shortUUIDCharArray.subSequence(12, 16))
      .append('-')
      .append(shortUUIDCharArray.subSequence(16, 20))
      .append('-')
      .append(shortUUIDCharArray.subSequence(20, 32))
      .toString())
  }
}
