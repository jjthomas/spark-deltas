package com.microsoft.dsoap.deltatests

import com.microsoft.dsoap.inputformat.{DsoapInputFormat, DsoapInputFormat2}
import edu.berkeley.cs.amplab.spark.indexedrdd.{IndexedRDD, LongSerializer, StringSerializer}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by t-jamth on 8/10/2015.
 */
object DeltaCompute {
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

  // V must define hashCode and equals
  def performNegDelta[V: ClassTag](a: Iterable[V], b: Iterable[V]): Iterable[V] = {
    var sourceMap = mutable.Map.empty[V, Int]
    for (e <- a) {
      val newEntry = (e, sourceMap.getOrElse(e, 0) + 1)
      sourceMap += newEntry
    }
    for (e <- b) {
      val newEntry = (e, sourceMap(e) - 1)
      sourceMap += newEntry
    }
    var result = ArrayBuffer.empty[V]
    for (kv <- sourceMap) {
      // safe to use multiple references to the same map since the RDD is immutable
      result ++= List.fill(kv._2)(kv._1)
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val GB_PART = 5
    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    val inputFormat = if (args(2).equals("true")) classOf[DsoapInputFormat2] else classOf[DsoapInputFormat]
    val query =
      s"""
         |{
         |   "Operation" : 2,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(0)}"}],
          |   "Filter" : [],
          |   "Description" : "query from spark"
          |}
      """.stripMargin
    jobConf.set("dsoap.query", query)
    implicit val longSer = new LongSerializer
    implicit val stringSer = new StringSerializer
    val tweets =  IndexedRDD(new DsoapRDD(sc.newAPIHadoopRDD(jobConf, inputFormat, classOf[LongWritable],
      classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      )))
    tweets.cache()
    // can avoid shuffle for groupByKey if we used DsoapInputFormat (not DsoapInputFormat2)
    // TODO expansion by user important in storage layer
    val userGrouped = IndexedRDD(tweets.map(t => (t._2("userid").toLong, t._2)).groupByKey(GB_PART))
    userGrouped.cache()
    val pairGrouped = IndexedRDD(userGrouped.flatMap(u => allPairs(u._2)).groupByKey(GB_PART))
    pairGrouped.cache()
    val results = pairGrouped.map(t => (t._1, t._2.sum))
    // TODO save results as IndexedRDD as well

    val deltaQ =
      s"""
         |{
         |   "Operation" : 3,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(1)}", "Previous" : "${args(0)}"}],
         |   "Filter" : [],
         |   "Description" : "query from spark"
         |}
      """.stripMargin
    jobConf.set("dsoap.query", deltaQ)
    val deltas = new DsoapRDD(sc.newAPIHadoopRDD(jobConf, inputFormat,
      classOf[LongWritable], classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      ))
    // TODO apply deltas to initial dataset
    val posDeltasUser = deltas.filter(t => !t._2.contains("delta")).map(t => (t._2("userid").toLong, t._2)).groupByKey(GB_PART)
    val negDeltasUser = tweets.select(deltas.filter(t => t._2.contains("delta"))).map(t => (t._2("userid").toLong, t._2)).groupByKey(GB_PART)
    posDeltasUser.cache()
    negDeltasUser.cache()
    val userGrouped2 = userGrouped.applyDelta(posDeltasUser){case (a, b) => a ++ b}
      .applyDelta(negDeltasUser){case (a, b) => performNegDelta(a, b)}
    userGrouped2.cache()
    // compute modified keys after applyingDeltas so that the deltas are materialized on the same
    // nodes where userGrouped is cached, if we flip the order the nodes where materialized will be
    // random and the RDDs will need to be recomputed when we apply delta
    // ^^ not true because this all turns into a single DAG (no action until end)
    // TODO believe that distinct will do no shuffling here since the keys are hash partitioned identically
    // TODO and distinct by default uses a hash partitioner with the same # of partitions as parents
    // ideally we could produce this at the same time as applyingDeltas, but Spark does not allow multiple
    // outputs from a transformation
    val changedKeys = posDeltasUser.map(t => (t._1, null)).union(negDeltasUser.map(t => (t._1, null))).distinct()
    // entries of changedKeys in userGrouped are negative deltas, entries in userGrouped2 are positive
    // TODO need to change select to handle missing keys
    val posDeltasPair = userGrouped2.select(changedKeys).flatMap(u => allPairs(u._2)).groupByKey(GB_PART)
    val negDeltasPair = userGrouped.select(changedKeys).flatMap(u => allPairs(u._2)).groupByKey(GB_PART)
    posDeltasPair.cache()
    negDeltasPair.cache()
    val pairGrouped2 = pairGrouped.applyDelta(posDeltasPair){case (a, b) => a ++ b}
      .applyDelta(negDeltasPair){case (a, b) => performNegDelta(a, b)}
    pairGrouped2.cache()

    // TODO before we apply the deltas need to make sure they are wrapped in DsoapRDD to avoid
    // TODO partitioning (false)
    // force complete materialization
    // println(tweets.reduce((a, b) => (a._1 + b._1, null))._1)
    println(sc.runJob(tweets, (c : TaskContext, i : Iterator[(Long, Map[String, String])]) => {
      val t = System.currentTimeMillis()
      val e = i.maxBy(t => t._1)
      println("Iteration time: " + (System.currentTimeMillis() - t))
      e
    }).mkString("\n"))
  }
}
