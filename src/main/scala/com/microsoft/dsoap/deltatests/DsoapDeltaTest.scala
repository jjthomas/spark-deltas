package com.microsoft.dsoap.deltatests

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.microsoft.dsoap.inputformat.{DsoapInputFormat, DsoapInputFormat2}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, DeltaComputation}
import org.apache.spark._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by t-jamth on 8/13/2015.
 */

object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}

object DsoapPartitioner extends Partitioner {
  // TODO if this partitioner is propagated through transformations it may be used for groupBy partitioning
  def numPartitions: Int = 0

  // never have to partition any data, so no need to define this
  // class is only useful as a marker to indicate existing partitioning
  def getPartition(key: Any): Int = 0

  override def equals(other: Any): Boolean = this == other

  override def hashCode: Int = 0
}

// used to wrap RDDs that have the partitioning specified
// in DsoapInputFormat(2) so that Spark knows that they are
// identically partitioned ... prevents shuffles e.g. when
// input IndexedRDDs are updated with deltas from DSoAP
class DsoapRDD[T: ClassTag](prev: RDD[T]) extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = Some(DsoapPartitioner)

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context)
}

object DsoapDeltaTest {
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

  // similar to ComputeOnlyDeltaTest except that records are actually fetched from
  // DSoAP rather than generated
  // queries to DSoAP are expanded by userid
  // args(0) -- initial query
  // args(1) -- second query (how similar its result set is to first determines perf. of deltas)
  // args(2) -- whether to clear DSoAP cache before issuing queries to it, so that end-to-end
  // measurements are more comparable between recomputation and deltas case
  // only two measurements here -- one for recomputation and one for deltas -- each
  // includes the time to fetch the source records from the store and run the initial
  // computation pipeline, and then the time to either recompute on the new set of records
  // or feed deltas through the pipeline
  def main(args: Array[String]): Unit = {
    if (args(2).equals("true")) {
      DsoapInputFormat.clearIndexCache();
    }

    val GB_PART = 5
    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    val inputFormat = classOf[DsoapInputFormat]

    var t = System.currentTimeMillis()
    val query =
      s"""
         |{
         |   "Operation" : 2,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(0)}"}],
         |   "Filter" : [{"ColumnName" : "text"}],
         |   "Expansion" : [{"ColumnName" : "userid"}],
         |   "Description" : "query from spark"
         |}
      """.stripMargin
    jobConf.set("dsoap.query", query)
    // see class comment on DsoapRDD for explanation of its purpose
    val tweets =  new DsoapRDD(sc.newAPIHadoopRDD(jobConf, inputFormat, classOf[LongWritable],
      classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      ))
    val results = tweets.map(t => (t._2("userid").toLong, t._2)).groupByKey(GB_PART)
      .flatMap(u => allPairs(u._2)).groupByKey(GB_PART)
      .map(t => (t._1, t._2.sum))
    println("initial: " + sc.runJob(results, (c : TaskContext, i : Iterator[(String, Int)]) => {
      i.maxBy(t => t._2)}).mkString(" "))

    val second =
      s"""
         |{
         |   "Operation" : 2,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(1)}"}],
         |   "Filter" : [{"ColumnName" : "text"}],
         |   "Expansion" : [{"ColumnName" : "userid"}],
         |   "Description" : "query from spark"
         |}
      """.stripMargin
    jobConf.set("dsoap.query", second)
    val tweets2 =  new DsoapRDD(sc.newAPIHadoopRDD(jobConf, inputFormat, classOf[LongWritable],
      classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      ))
    val results2 = tweets2.map(t => (t._2("userid").toLong, t._2)).groupByKey(GB_PART)
      .flatMap(u => allPairs(u._2)).groupByKey(GB_PART)
      .map(t => (t._1, t._2.sum))
    println("final: " + sc.runJob(results2, (c : TaskContext, i : Iterator[(String, Int)]) => {
      i.maxBy(t => t._2)}).mkString(" "))
    println("Recomputation time: " + (System.currentTimeMillis() - t))

    if (args(2).equals("true")) {
      DsoapInputFormat.clearIndexCache();
    }

    t = System.currentTimeMillis()
    val sourceDc = DeltaComputation.newInstance(results, tweets)
    println("initial w/ delta: " + sc.runJob(sourceDc.getTransformed, (c : TaskContext, i : Iterator[(String, Int)]) => {
      i.maxBy(t => t._2)}).mkString(" "))

    val deltaQ =
      s"""
         |{
         |   "Operation" : 3,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(1)}", "Previous" : "${args(0)}"}],
         |   "Filter" : [{"ColumnName" : "text"}],
         |   "Expansion" : [{"ColumnName" : "userid"}],
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
    val posDeltas = deltas.filter(t => !t._2.contains("delta"))
    val negDeltas = deltas.filter(t => t._2.contains("delta"))
    val newDc = sourceDc.applyDeltas(posDeltas, negDeltas)
    println("final w/ delta: " + sc.runJob(newDc.getTransformed, (c : TaskContext, i : Iterator[(String, Int)]) => {
      i.maxBy(t => t._2)}).mkString(" "))
    println("Delta time: " + (System.currentTimeMillis() - t))
  }
}
