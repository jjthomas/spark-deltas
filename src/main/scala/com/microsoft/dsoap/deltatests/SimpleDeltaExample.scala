package com.microsoft.dsoap.deltatests

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.DeltaComputation
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

/**
 * Created by t-jamth on 8/18/2015.
 */
object SimpleDeltaExample {

  def main(args: Array[String]): Unit = {
    val PARTS = args(0).toInt

    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query Compute-Only")
    val sc = new SparkContext(conf)
    val source = sc.parallelize(Seq((1.toLong, 1.toLong), (2.toLong, 2.toLong)))
      .partitionBy(new HashPartitioner(PARTS))
    source.cache()
    println("initial: " + sc.runJob(source,
      (c: TaskContext, i: Iterator[(Long, Long)]) => {
        System.err.println("initial")
        i.mkString(" ")
      }).mkString("//"))
    val result = source.map(t => (t._2, t._1)).groupByKey(PARTS).mapValues(v => v.sum)
    println("final: " + sc.runJob(result,
      (c: TaskContext, i: Iterator[(Long, Long)]) => {
        System.err.println("final")
        i.mkString(" ")
      }).mkString("//"))

    val sourceDc = DeltaComputation.newInstance(result, source)
    println("initial w/ delta: " + sc.runJob(sourceDc.getTransformed,
      (c: TaskContext, i: Iterator[(Long, Long)]) => {
        System.err.println("initial w/ delta")
        i.mkString(" ")
      }).mkString("//"))

    val neg = sc.parallelize(Seq((1.toLong, 1.toLong))).partitionBy(new HashPartitioner(PARTS))
    neg.cache()
    println("neg: " + sc.runJob(neg,
      (c: TaskContext, i: Iterator[(Long, Long)]) => {
        System.err.println("neg")
        i.mkString(" ")
      }).mkString("//"))
    val pos = sc.parallelize(Seq((3.toLong, 3.toLong))).partitionBy(new HashPartitioner(PARTS))
    pos.cache()
    println("pos: " + sc.runJob(pos,
      (c: TaskContext, i: Iterator[(Long, Long)]) => {
        System.err.println("pos")
        i.mkString(" ")
      }).mkString("//"))

    println("final w/ delta: " + sc.runJob(sourceDc.applyDeltas(pos, neg).getTransformed,
      (c: TaskContext, i: Iterator[(Long, Long)]) => {
        System.err.println("final w/ delta")
        i.mkString(" ")
      }).mkString("//"))
  }


    /*
    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query Compute-Only")
    val sc = new SparkContext(conf)
    sc.parallelize(0 until args(0).toInt).map(i => (i.toLong, (i % args(1).toInt).toLong))
      .partitionBy(new HashPartitioner(5))
    */

}
