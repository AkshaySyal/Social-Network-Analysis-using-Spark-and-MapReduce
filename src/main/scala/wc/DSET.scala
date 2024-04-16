package wc

import org.apache.spark.sql.{SparkSession, DataFrame}

object DSETMain {
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Follower Count")
      .getOrCreate()

    // Setting log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    if (args.length != 3) {
      println("Usage:\nwc.DSETMain <input dir> <output dir>")
      System.exit(1)
    }

    // Read input CSV file into DataFrame
    val df: DataFrame = spark.read
      .format("csv")
      .option("inferSchema", "true") // Infer schema from data types
      .load(args(0))
      .toDF("follower", "followee") // Specify column names

    df.explain()

    // Filter rows where the value of the "followee" column is divisible by 100, then group by "followee" and count occurrences
    import spark.implicits._
    val countDF = df.filter($"followee" % 100 === 0).groupBy("followee").count()

    // Save the result to the output directory
    countDF.write.format("csv").save(args(1))

  }
}
