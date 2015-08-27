package com.microsoft.dsoap.misctest

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by t-jamth on 6/24/2015.
 */
object Count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP HDFS Counter").set("spark.locality.wait", "3000")
    val sc = new SparkContext(conf)
    // Is using Spark SQL inefficient here?
    println("COUNT: " + sc.textFile("hdfs:///datasets/twitter/2014/12/*/*").count())
  }
}
