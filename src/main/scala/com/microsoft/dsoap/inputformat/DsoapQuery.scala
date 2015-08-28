package com.microsoft.dsoap.inputformat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by t-jamth on 7/20/2015.
 */
object DsoapQuery {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP Spark Query")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    // add "Bucket" : "%s" to use DsoapInputFormat2
    val query =
      s"""
        |{
        |   "Operation" : 2,
        |   "Search" : [{"IdxStoreName" : "Text", "Query" : "${args(0)}"}],
        |   "Filter" : [{"ColumnName" : "text"}],
        |   "Description" : "query from spark"
        |}
      """.stripMargin
    jobConf.set("dsoap.query", query)
    val tweets = sc.newAPIHadoopRDD(jobConf, classOf[DsoapInputFormat], classOf[LongWritable],
      classOf[Text])

    val sqlContext = new SQLContext(sc)
    val json = sqlContext.jsonRDD(tweets.map(pair => pair._2.toString))
    json.registerTempTable("tweets")
    // some fake feature extractors that are applied to the text
    sqlContext.registerFunction("wordCount", (s: String) => s.split(' ').length / 20)
    sqlContext.registerFunction("activity", (s: String) => s.split(' ')(0))
    val expanded = sqlContext.sql("SELECT *, wordCount(text) AS wc, activity(text) as activity FROM tweets")
    println(expanded.take(1)(0))
  }
}
