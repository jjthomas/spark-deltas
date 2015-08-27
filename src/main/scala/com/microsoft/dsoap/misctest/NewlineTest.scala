package com.microsoft.dsoap.misctest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by t-jamth on 7/10/2015.
 */
object NewlineTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP HDFS Newline")
    val sc = new SparkContext(conf)
    // Is using Spark SQL inefficient here?
    val in = sc.textFile("hdfs:///datasets/twitter/2014/11/06")
    println(in.flatMap(t => if (t.contains("530461334761406464")) Seq(t) else Seq()).collect()(0))
    println("...RESULT")
  }
}
