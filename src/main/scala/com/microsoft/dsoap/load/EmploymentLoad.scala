package com.microsoft.dsoap.load

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text, NullWritable}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by t-jamth on 7/10/2015.
 */

class EmploymentMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  // number of user splits in the output
  final val USER_SPLITS = 4;

  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    "user-" + (key.asInstanceOf[Long] % USER_SPLITS) + "/" + name
}

/**
 * args(0) path of HDFS input file or directory (if directory, all files in the directory tree
 * are used as input), e.g. hdfs:///datasets/twitter/2014/12/31
 * args(1) path of HDFS output directory, in which one output directory per user split (see above
 * to change number of user splits) will be written, e.g. hdfs:///user/t-jamth/twitter-dump/user-splits
 */
object EmploymentLoad {

  def isLong(x: String) = x forall Character.isDigit

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP HDFS Loader")
    val sc = new SparkContext(conf)
    val hConf = new Configuration(sc.hadoopConfiguration)
    // records are split this way in these datasets
    hConf.set("textinputformat.record.delimiter", "\r\n")
    val data = sc.newAPIHadoopFile(args(0),
      classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hConf)

    data.map(t => t._2.toString).flatMap {
      text =>
        val str = text.replace('\n', ' ').replace('\r', ' ')
        val split = str.split('\t')
        // should have 15 fields, user ID is the last
        if (split.length == 15) {
          val el14 = str.split('\t')(14).trim
          if (!isLong(el14)) Seq() else Seq((el14.toLong, str))
        } else {
          Seq()
        }
    }.saveAsHadoopFile(args(1), classOf[String], classOf[String], classOf[EmploymentMultipleTextOutputFormat])
  }
}
