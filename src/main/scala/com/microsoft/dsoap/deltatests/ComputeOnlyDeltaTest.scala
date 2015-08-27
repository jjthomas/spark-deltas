package com.microsoft.dsoap.deltatests

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{DeltaComputation, RDD}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by t-jamth on 8/14/2015.
 */
object ComputeOnlyDeltaTest {
  def allPairs(maps : Iterable[Map[String, String]]): TraversableOnce[(String, Int)] = {
    var s = mutable.Set.empty[String]
    for (m <- maps) {
      s ++= m("text").split("(,|\\s)").filter(s => !s.isEmpty)
    }
    val seq = s.toSeq.sorted
    var buf = ArrayBuffer.empty[(String, Int)]
    for (i <- 0 until seq.length) {
      for (j <- i + 1 until seq.length) {
        val newEntry = (seq(i) + "," + seq(j), 1)
        buf += newEntry
      }
    }
    buf
  }

  def main(args: Array[String]): Unit = {
    // num records in original dataset
    val NUM_IDS = args(0).toInt
    // number of groups, records distributed (roughly) uniformly over groups
    val NUM_USERS = args(1).toInt
    // number of positive deltas
    val NUM_POS = args(2).toInt
    // number of negative deltas
    val NUM_NEG = args(3).toInt
    // number of partitions used in shuffles
    val GB_PART = args(4).toInt
    // number of words in fake vocabulary used to generate text for each record
    val W = args(5).toInt

    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query Compute-Only")
    val sc = new SparkContext(conf)

    def getSourceResult(range : Seq[Int]): (RDD[(Long, Map[String, String])], RDD[(String, Int)]) = {
      // get a synthetic dataset with the IDs in range as well as the result of passing it through
      // a computation pipeline
      val source =
        sc.parallelize(range, GB_PART).map(i => {
          val r = new Random(i)
          (i.toLong, Map("id" -> i.toString, "userid" -> r.nextInt(NUM_USERS).toString,
            "text" -> "%d %d %d".format(r.nextInt(W), r.nextInt(W), r.nextInt(W))))})
          // partition so that delta updates to the input don't require shuffles,
          // which is what happens in case of reading from DSoAP
          .partitionBy(new HashPartitioner(GB_PART))
      source.cache()
      // materialize and cache source now so that generating it doesn't factor
      // into the measurements below -- we want to measure only time taken in the
      // computation pipeline
      sc.runJob(source, (c: TaskContext, i: Iterator[(Long, Map[String, String])]) => {
        i.maxBy(t => t._1)})
      val result = source.map(t => (t._2("userid").toLong, t._2)).groupByKey(GB_PART)
        .flatMap(u => allPairs(u._2)).groupByKey(GB_PART)
        .mapValues(v => v.sum)
      (source, result)
    }

    // all of the runJob calls materialize the full output dataset

    // time for the computation pipeline on the input dataset
    val (source, result) = getSourceResult(0 until NUM_IDS)
    var t = System.currentTimeMillis()
    println("initial: " + sc.runJob(result, (c : TaskContext, i : Iterator[(String, Int)]) => {
      i.maxBy(t => t._2)}).mkString(" "))
    println("initial time: " + (System.currentTimeMillis() - t))

    // time for the computation pipeline on the input dataset, with IndexedRDD's
    // added to the pipeline to save state at the input, output, and after shuffles
    val sourceDc = DeltaComputation.newInstance(result, source)
    t = System.currentTimeMillis()
    println("initial w/ delta: " + sc.runJob(sourceDc.getTransformed,
      (c : TaskContext, i : Iterator[(String, Int)]) => {i.maxBy(t => t._2)}).mkString(" "))
    println("initial w/ delta time: " + (System.currentTimeMillis() - t))

    // time for the computation on the updated dataset with full recomputation
    val (_, result2) = getSourceResult(NUM_NEG until NUM_IDS + NUM_POS)
    t = System.currentTimeMillis()
    println("final: " + sc.runJob(result2, (c : TaskContext, i : Iterator[(String, Int)]) => {
      i.maxBy(t => t._2)}).mkString(" "))
    println("final time: " + (System.currentTimeMillis() - t))

    // time for the computation on the updated datasets if deltas used
    val (neg, _) = getSourceResult(0 until NUM_NEG)
    val (pos, _) = getSourceResult(NUM_IDS until NUM_IDS + NUM_POS)
    t = System.currentTimeMillis()
    println("final w/ delta: " + sc.runJob(sourceDc.applyDeltas(pos, neg).getTransformed,
      (c : TaskContext, i : Iterator[(String, Int)]) => {i.maxBy(t => t._2)}).mkString(" "))
    println("final w/ delta time: " + (System.currentTimeMillis() - t))
  }
}
