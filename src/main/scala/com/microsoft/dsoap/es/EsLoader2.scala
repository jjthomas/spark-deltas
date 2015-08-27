package com.microsoft.dsoap.es

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * Created by t-jamth on 8/24/2015.
 */
object EsLoader2 {
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

    // val tweets = sqlContext.jsonFile("hdfs:///datasets/twitter/2014/12/31/twitter_updates_2014_12_31_23.log_bucket9")hhl
    val tweets = sc.textFile(args(0))
    val fields = Seq("ID", "CreatedAt", "Text", "UserName", "UserID", "UserDescription", "UserCreatedAt",
      "FavoriteCount", "GeoPoint", "InReplyToStatusId", "InReplyToUserId",
      "RetweetedID", "RetweetedUserID", "Source", "UserFavoritesCount", "UserFollowersCount",
      "UserFriendsCount", "UserLocation", "UserScreenName", "UserStatusesCount", "UserTimeZone", "UserUrl",
      "UserUtcOffset", "UserProfileImageUrl")
    val userDefined = Seq("Text", "UserName", "UserDescription", "UserLocation", "UserScreenName")
    tweets.flatMap(r => {
      val values = r.split("\t")
      // may want the IDs to be longs instead of strings for ES ingestion performance
      if (values.length == 24) {
        values(8) = values(8).replace(' ', ',')
        val m = new MapWritable
        for ((field, value) <- fields.zip(values)) {
          m.put(new Text(field), if ((value.equals("null") && !userDefined.contains(field)) || value.isEmpty) NullWritable.get() else new Text(value))
        }
        Seq((NullWritable.get, m))
      } else {
        Seq()
      }
    }).saveAsHadoopDataset(jobConf)
  }
}
