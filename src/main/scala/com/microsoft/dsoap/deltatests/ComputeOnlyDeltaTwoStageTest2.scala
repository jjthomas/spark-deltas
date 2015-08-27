package com.microsoft.dsoap.deltatests

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{DeltaComputation, RDD}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

/**
 * Created by t-jamth on 8/21/2015.
 */
object ComputeOnlyDeltaTwoStageTest2 {
  def dummyComputation(l: Long): Long = {
    /*
    val r = new Random(l)
    for (i <- 1 to 100000) {
      r.nextLong()
    }
    r.nextLong()
    */
    return l
  }

  // see ComputeOnlyDeltaTwoStageTest for documentation, this uses simpler records
  // (no text), computation is slightly different,
  // and also exact uniform distribution of IDs over users
  def main(args: Array[String]): Unit = {
    val NUM_IDS = args(0).toInt
    val NUM_USERS = args(1).toInt
    val NUM_POS = args(2).toInt
    val NUM_NEG = args(3).toInt
    val GB_PART = args(4).toInt

    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query Compute-Only")
    val sc = new SparkContext(conf)

    def getSourceResult(range : Seq[Int]): (RDD[(Long, Long)], RDD[(Long, Long)]) = {
      val source =
        sc.parallelize(range, GB_PART).map(i => (i.toLong, (i % NUM_USERS).toLong))
          .partitionBy(new HashPartitioner(GB_PART))
      source.cache()
      sc.runJob(source, (c: TaskContext, i: Iterator[(Long, Long)]) => {
        i.maxBy(t => t._1)})
      val result = source.map(t => (t._2, t._1)).groupByKey(GB_PART)
        .mapValues(v => v.map(l => dummyComputation(l)).sum)
      (source, result)
    }

    val (source, result) = getSourceResult(0 until NUM_IDS)
    var t = System.currentTimeMillis()
    println("initial: " + sc.runJob(result, (c : TaskContext, i : Iterator[(Long, Long)]) => {
      i.maxBy(t => t._1)}).mkString(" "))
    println("initial time: " + (System.currentTimeMillis() - t))

    val sourceDc = DeltaComputation.newInstance(result, source)
    t = System.currentTimeMillis()
    println("initial w/ delta: " + sc.runJob(sourceDc.getTransformed,
      (c : TaskContext, i : Iterator[(Long, Long)]) => {i.maxBy(t => t._1)}).mkString(" "))
    println("initial w/ delta time: " + (System.currentTimeMillis() - t))

    val (_, result2) = getSourceResult(NUM_NEG until NUM_IDS + NUM_POS)
    t = System.currentTimeMillis()
    println("final: " + sc.runJob(result2, (c : TaskContext, i : Iterator[(Long, Long)]) => {
      i.maxBy(t => t._1)}).mkString(" "))
    println("final time: " + (System.currentTimeMillis() - t))

    val (neg, _) = getSourceResult(0 until NUM_NEG)
    val (pos, _) = getSourceResult(NUM_IDS until NUM_IDS + NUM_POS)
    t = System.currentTimeMillis()
    println("final w/ delta: " + sc.runJob(sourceDc.applyDeltas(pos, neg).getTransformed,
      (c : TaskContext, i : Iterator[(Long, Long)]) => {i.maxBy(t => t._1)}).mkString(" "))
    println("final w/ delta time: " + (System.currentTimeMillis() - t))
  }
}
