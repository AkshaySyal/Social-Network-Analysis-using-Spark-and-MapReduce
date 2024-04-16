package wc
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDGMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.RDDGMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Follower Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS.
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }

    // Read the input text file and create an RDD of (userID, followerID)
    val textFile = sc.textFile(args(0))
    val userNumOfFollowersRDD = textFile.map(line => line.split(","))
                                    .map(fields => (fields(1).toInt,fields(0).toInt))
                                    .filter { case (userId, _) => userId % 100 == 0 }
                                    .groupByKey()
                                    .mapValues(_.size)
    
    // Print the debug string for groupedRDD
    println("Debug String for userNumOfFollowersRDD:")
    println(userNumOfFollowersRDD.toDebugString)   
    
    userNumOfFollowersRDD.saveAsTextFile(args(1))

  }
}
