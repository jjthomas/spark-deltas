package com.microsoft.dsoap.misctest

import com.microsoft.dsoap.deltatests.JsonUtil
import com.microsoft.dsoap.inputformat.{DsoapInputFormat, DsoapInputFormat2}
import edu.berkeley.cs.amplab.spark.indexedrdd.{IndexedRDD, LongSerializer}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * Created by t-jamth on 8/7/2015.
 */
object DsoapFailureTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP Spark Delta Query Test")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    val inputFormat = if (args(2).equals("true")) classOf[DsoapInputFormat2] else classOf[DsoapInputFormat]
    val query =
      s"""
         |{
         |   "Operation" : 2,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(0)}"}],
          |    "Filter" : [],
          |   "Description" : "query from spark"
          |}
      """.stripMargin
    jobConf.set("dsoap.query", query)
    // IndexedRDD(new DsoapRDD(
    val tweets =  sc.newAPIHadoopRDD(jobConf, inputFormat, classOf[LongWritable],
      classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      )
    // tweets.cache()
    // force materialization
    println(sc.runJob(tweets, (c : TaskContext, i : Iterator[(Long, Map[String, String])]) => i.take(1).next).mkString("\n"))
    val deltaQ =
      s"""
         |{
         |   "Operation" : 2,
         |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(1)}", "Previous" : "${args(0)}"}],
         |   "Filter" : [],
         |   "Description" : "query from spark"
         |}
      """.stripMargin
    jobConf.set("dsoap.query", deltaQ)
    // new DsoapRDD(
    implicit val longSer = new LongSerializer
    val updatedTweets : RDD[(Long, Map[String, String])] = IndexedRDD(sc.newAPIHadoopRDD(jobConf, inputFormat,
      classOf[LongWritable], classOf[Text]).map(
        t => {
          val m = JsonUtil.fromJson[Map[String, String]](t._2.toString)
          (m("id").toLong, m)
        }
      ))
    // force materialization
    println(sc.runJob(updatedTweets, (c : TaskContext, i : Iterator[(Long, Map[String, String])]) => i.take(1).next).mkString("\n"))
  }
}
