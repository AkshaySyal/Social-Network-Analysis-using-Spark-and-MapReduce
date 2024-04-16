package wc
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

object RSRDD {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.RDDRMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Triangle Count")
    val sc = new SparkContext(conf)
    val MAX_FILTER = args(2).toLong


    // Delete output directory, only to ease local development; will not work on AWS.
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }

    // Read the input text file and create RDDs of fromTo->(node1,node2)

    val nodesCSV = sc.textFile(args(0))

    val filteredCSV = nodesCSV.filter(nodeLine => {
      val nodes = nodeLine.split(",")
      val follower = nodes(0).toLong
      val followed = nodes(1).toLong

      follower.toLong < MAX_FILTER && followed.toLong < MAX_FILTER
    })

    val followerFollowed = filteredCSV.map(nodeLine =>{
      val nodes = nodeLine.split(",")
      val follower = nodes(0).toLong
      val followed = nodes(1).toLong

      (follower,followed)
    })

    val followedFollower = followerFollowed.map { case (key, value) =>
      (value, key)
    }

    val traingleCounter = sc.longAccumulator("Triangle Counter")

    val path2 = joinOnKey(followedFollower,followerFollowed).map(_._2)

    val triangles = joinOnKey(path2,followedFollower).map(_._2)

    triangles.foreach {t=>if(t._1 == t._2) traingleCounter.add(1)}
    println("Number of Triangles: " + traingleCounter.value / 3)

    val triangleCount = traingleCounter.value / 3
    sc.parallelize(Seq(triangleCount)).saveAsTextFile(args(1))
  }

  def joinOnKey(fromRDD: RDD[(Long, Long)],
                toRDD: RDD[(Long, Long)]): RDD[(Long, (Long, Long))] = {
    val joinedRDD = fromRDD.join(toRDD)
    joinedRDD
  }



}
