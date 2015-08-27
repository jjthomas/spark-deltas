package com.microsoft.dsoap.misctest

import scala.util.Random

/**
 * Created by t-jamth on 8/21/2015.
 */
object TestRand {
  def main(args: Array[String]): Unit = {
    val r = new Random(0)
    val t = System.currentTimeMillis()
    for (i <- 1 to 1000) {
      r.nextLong()
    }
    println(System.currentTimeMillis() - t)
  }
}
