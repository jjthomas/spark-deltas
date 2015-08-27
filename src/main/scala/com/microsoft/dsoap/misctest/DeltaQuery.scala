package com.microsoft.dsoap.misctest

import com.microsoft.dsoap.deltatests.{DsoapRDD, JsonUtil}
import com.microsoft.dsoap.inputformat.{DsoapInputFormat, DsoapInputFormat2}
import edu.berkeley.cs.amplab.spark.indexedrdd.{IndexedRDD, LongSerializer}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * Created by t-jamth on 8/4/2015.
 */

object DeltaQuery {
  def main(args: Array[String]): Unit = {
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
    val tweets =  IndexedRDD(new DsoapRDD(sc.newAPIHadoopRDD(jobConf, inputFormat, classOf[LongWritable],
      classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      )))
    tweets.cache()
    // force complete materialization
    // println(tweets.reduce((a, b) => (a._1 + b._1, null))._1)
    println(sc.runJob(tweets, (c : TaskContext, i : Iterator[(Long, Map[String, String])]) => {
      val t = System.currentTimeMillis()
      val e = i.maxBy(t => t._1)
      println("Iteration time: " + (System.currentTimeMillis() - t))
      e
    }).mkString("\n"))
    val doDelta = args(3).equals("true")
    val deltaQ =
      s"""
         |{
         |   "Operation" : ${if (doDelta) 3 else 2},
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(1)}", "Previous" : "${args(0)}"}],
         |   "Filter" : [],
         |   "Description" : "query from spark"
         |}
      """.stripMargin
    jobConf.set("dsoap.query", deltaQ)
    var updatedTweets : RDD[(Long, Map[String, String])] = new DsoapRDD(sc.newAPIHadoopRDD(jobConf, inputFormat,
      classOf[LongWritable], classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      ))
    if (doDelta) updatedTweets = tweets.applyDelta(updatedTweets){case (_, _) => null}
    // force complete materialization
    // println(updatedTweets.reduce((a, b) => (a._1 + b._1, null))._1)
    println(sc.runJob(updatedTweets, (c : TaskContext, i : Iterator[(Long, Map[String, String])]) => {
      val t = System.currentTimeMillis()
      val e = i.maxBy(t => t._1)
      println("Iteration time: " + (System.currentTimeMillis() - t))
      e
    }).mkString("\n"))
  }
}
