package programmerby

import org.apache.spark._
import spray.json._
import DefaultJsonProtocol._

// Define the schema using a case class
case class SparkProblem1Friends(n1: String, n2: String)

// Run:
// spark-submit --class programmerby.SparkProblem1 --master local path-to/hadoop-programmerby-1.0.jar path-to/links.json
object SparkProblem1 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <input>")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.setMaster(conf.get("spark.master", "local"))
      .setAppName(this.getClass.getSimpleName)

    val spark = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    import sqlContext._
    import sqlContext.implicits._

    // Unfortunately, sqlContext.jsonFile can't parse provided JSON data
    val friends = spark.textFile(args(0))
      .map(l => l.parseJson.convertTo[List[String]])
      .map(p => SparkProblem1Friends(p(0), p(1)))
      .toDF()
    friends.registerTempTable("friends")
    // friends.printSchema
    // friends.show(999)

    // Method 1: RDD
    println("Method 1: RDD")

    friends.rdd.flatMap(row => List(((row.getString(0), row.getString(1)), 1), ((row.getString(1), row.getString(0)), -1)))
      .reduceByKey((v1, v2) => v1 + v2)
      .filter(row => row._2 > 0)
      .sortByKey()
      .map(row => row._1)
      .collect().foreach(println)

    // Method 2: Spark SQL
    println("Method 2: Spark SQL")

    val nonSymmetricFriends = sqlContext.sql("SELECT z1.n1, z1.n2\n" +
      "FROM friends z1\n" +
      "LEFT JOIN friends z2 ON (z1.n1 = z2.n2 AND z1.n2 = z2.n1)\n" +
      "WHERE z2.n1 IS NULL\n" +
      "ORDER BY z1.n1, z1.n2")

    nonSymmetricFriends.collect().foreach(println)

    // Method 3: Spark SQL's Data Frame
    println("Method 3: DataFrame")

    val z1 = friends.as("z1")
    val z2 = friends.as("z2")

    z1.join(z2, $"z1.n2" === $"z2.n1" && $"z1.n1" === $"z2.n2", "left")
      .filter($"z2.n1" isNull)
      .select($"z1.n1", $"z1.n2")
      .orderBy("n1", "n2")
      .collect().foreach(println)
  }
}
