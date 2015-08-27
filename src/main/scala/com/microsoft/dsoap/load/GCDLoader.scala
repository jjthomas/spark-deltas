package com.microsoft.dsoap.load

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  // number of user splits in the output
  final val USER_SPLITS = 24;

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
object GCDLoader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSOAP HDFS Loader")
    val sc = new SparkContext(conf)
    // TODO is using Spark SQL inefficient here?
    val sqlContext = new SQLContext(sc)
    val tweets = sqlContext.jsonFile(args(0))
    tweets.registerTempTable("tweets")
    val reduced = sqlContext.sql("SELECT ID, CreatedAt, Text, User.Name, User.ID, User.Description, User.CreatedAt, " +
        "FavoriteCount, GeoPoint, InReplyToStatusId, InReplyToUserId, " +
        "RetweetedID, RetweetedUserID, Source, User.FavoritesCount, User.FollowersCount, " +
        "User.FriendsCount, User.Location, User.ScreenName, User.StatusesCount, User.TimeZone, User.Url, " +
        "User.UtcOffset, User.ProfileImageUrl " +
        "FROM tweets WHERE Language = 'en' AND RetweetCount = 0")
    reduced.map(r => {
      var seq = r.toSeq
      val tDate = seq(1).toString
      val uDate = seq(6).toString
      seq = seq.updated(1, tDate.substring(tDate.indexOf('(') + 1, tDate.indexOf(')')))
               .updated(6, uDate.substring(uDate.indexOf('(') + 1, uDate.indexOf(')')))
               .map(e => {
                 if (e.isInstanceOf[String]) {
                   e.toString.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
                 } else {
                   e
                 }
               })
      (seq(4).toString.toLong, seq.mkString("\t"))
    })
    .saveAsHadoopFile(args(1), classOf[String], classOf[String],
      classOf[RDDMultipleTextOutputFormat])
  }
}
