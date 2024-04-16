package wc

import org.apache.spark.sql.{SparkSession, DataFrame}

object RepD {
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Triangle Count")
      .getOrCreate()

    // Setting log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    if (args.length != 3) {
      println("Usage:\nwc.RepD <input dir> <output dir>")
      System.exit(1)
    }

    

    val MAX_FILTER = args(2).toLong
  
    // Read input CSV file into DataFrame
    val edgesDF: DataFrame = spark.read
      .format("csv")
      .option("inferSchema", "true") // Infer schema from data types
      .load(args(0))
      .toDF("follower", "followee") // Specify column names
    
    import spark.implicits._

    import org.apache.spark.sql.functions._
    val filteredEdgesDF = edgesDF
    .filter(col("follower") < MAX_FILTER && col("followee") < MAX_FILTER)

    // Broadcasting edgesDF
    import org.apache.spark.sql.functions.broadcast

    val bcfilteredEdgesDF = broadcast(filteredEdgesDF)

    val path2DF = filteredEdgesDF.as("E1")
                  .join(bcfilteredEdgesDF.as("E2")) // Broadcasting E2 DataFrame
                  .where($"E1.followee" === $"E2.follower" && $"E1.follower" =!= $"E2.followee")
                  .select($"E1.follower".alias("E1Follower"), 
                          $"E2.follower".alias("E2Follower"), 
                          $"E2.followee".alias("E2followee"))

    val triangles = path2DF
                .join(bcfilteredEdgesDF,
                  path2DF("E2followee") === bcfilteredEdgesDF("follower") &&
                  path2DF("E1follower") === bcfilteredEdgesDF("followee"))
                  .count() / 3              

    // path2DF.explain(true)
    println(s"Number of triangles: $triangles")
    
    spark.sparkContext.parallelize(Seq(triangles.toString)).saveAsTextFile(args(1))

  }
}
