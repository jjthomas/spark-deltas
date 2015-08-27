package com.microsoft.dsoap.es

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * Created by t-jamth on 8/24/2015.
 */
object EsLoader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP ES Loader")
    val sc = new SparkContext(conf)
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE, "twitter/tweet")
    // TODO fill in nodes
    jobConf.set(ConfigurationOptions.ES_NODES, "XXX")
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    val sqlContext = new SQLContext(sc)
    // val tweets = sqlContext.jsonFile("hdfs:///datasets/twitter/2014/12/31/twitter_updates_2014_12_31_23.log_bucket9")
    val tweets = sqlContext.jsonFile(args(0))
    tweets.registerTempTable("tweets")
    val fields = Seq("ID", "CreatedAt", "Text", "User.Name", "User.ID", "User.Description", "User.CreatedAt",
      "FavoriteCount", "GeoPoint", "InReplyToStatusId", "InReplyToUserId",
      "RetweetedID", "RetweetedUserID", "Source", "User.FavoritesCount", "User.FollowersCount",
      "User.FriendsCount", "User.Location", "User.ScreenName", "User.StatusesCount", "User.TimeZone", "User.Url",
      "User.UtcOffset", "User.ProfileImageUrl")
    val reduced = sqlContext.sql("SELECT " + fields.mkString(",") + " FROM tweets WHERE Language = 'en' AND RetweetCount = 0")
    reduced.map(r => {
      val m = new MapWritable
      val initial = r.toSeq.map(e => if (e != null) e.toString else null)
      val tDate = initial(1)
      val uDate = initial(6)
      val tDateStrip = tDate.substring(tDate.indexOf('(') + 1, tDate.indexOf(')'))
      val uDateStrip = uDate.substring(uDate.indexOf('(') + 1, uDate.indexOf(')'))
      val seq = initial.updated(1, tDate).updated(6, uDate)
      // may want the IDs to be longs instead of strings for ES ingestion performance
      for ((field, value) <- fields.zip(seq)) {
        m.put(new Text(field.replace(".", "")), if (value == null || value.isEmpty) null else new Text(value))
      }
      (NullWritable.get, m)
    }).saveAsHadoopDataset(jobConf)
  }
}
