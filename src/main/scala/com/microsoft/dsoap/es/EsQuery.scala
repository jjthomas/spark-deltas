package com.microsoft.dsoap.es

import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsInputFormat

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Created by t-jamth on 6/30/2015.
 */

object EsTwitterPartitioner extends Partitioner {
  def numPartitions: Int = 0

  // never have to partition any data, so no need to define this
  // class is only useful as a marker to indicate existing partitioning
  def getPartition(key: Any): Int = 0

  override def equals(other: Any): Boolean = this == other

  override def hashCode: Int = 0
}

class EsTwitterRDD[T: ClassTag](prev: RDD[T]) extends RDD[T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = Some(EsTwitterPartitioner)

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context)
}

object EsQuery {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP ES Query")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set(ConfigurationOptions.ES_RESOURCE, "twitter/tweet")
    // TODO fill in nodes
    jobConf.set(ConfigurationOptions.ES_NODES, "XXX")
    val query =
      """
        |{
        |    "query" : {
        |        "filtered" : {
        |            "filter" : { "term" : { "Text" : "obama" } }
        |        }
        |    }
        |}
      """.stripMargin
    jobConf.set(ConfigurationOptions.ES_QUERY, query)
    // TODO preferredLocations in ESHadoop seems to be messed up ... supplies IP address when
    // TODO host is required ... see TaskSetManager.addPendingTask
    val tweets = sc.hadoopRDD(jobConf, classOf[EsInputFormat[Object, MapWritable]], classOf[Object],
                              classOf[MapWritable])
    val formatted = tweets.map{case (k, v) => v.map{case (mwk, mwv) => (mwk.toString, mwv.toString)}.toMap}
    val keyed = new EsTwitterRDD(formatted.map(m => (m.get("UserID"), m))).groupByKey(EsTwitterPartitioner)
    // Two stages (rather than one) if we uncomment the below:
    // val keyed = formatted.map(m => (m.get("UserID"), m)).groupByKey()
    println(keyed.collect().mkString("\n"))
  }
}
