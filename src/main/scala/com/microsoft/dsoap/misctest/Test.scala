package com.microsoft.dsoap.misctest

/**
 * Created by t-jamth on 7/21/2015.
 */
object Test {
  def main(args: Array[String]): Unit = {
    val x = Seq(0, 1, 2)
    println(x.flatMap(t => if (t == 0) Seq() else Seq(1)))
  }

}
