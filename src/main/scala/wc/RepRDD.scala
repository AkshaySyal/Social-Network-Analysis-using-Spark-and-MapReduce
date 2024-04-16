package wc
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner


object RepRDD {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.RepRDD <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Triangle Count")
    val sc = new SparkContext(conf)
    val MAX_FILTER = args(2).toLong

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
  
    val broadCastfollowerFollowed = followerFollowed.collect()
                                      .groupBy { case (follower, followed) => follower }
                        
    val broadCastedfollowerFollowed = sc.broadcast(broadCastfollowerFollowed)

    // Access the broadcasted value
    val broadcastedValue = broadCastedfollowerFollowed.value

  
    val path2 = followedFollower.flatMap {
      case(kL, vL) => broadCastedfollowerFollowed.value.get(kL) match {
        case Some(vS) => vS.map(element => (vL, element))
        case None => List.empty // Return an empty list if no value is found for kL
      }
    }

    
    val triangles = followedFollower.join(path2)

    

    triangles.foreach {case (key, (n1,(n2,n3))) => if(n1 == n3) traingleCounter.add(1)}

    val triangleCount = traingleCounter.value / 3
    sc.parallelize(Seq(triangleCount)).saveAsTextFile(args(1))

    println("Number of Triangles: " + traingleCounter.value / 3)


  }

  

}
