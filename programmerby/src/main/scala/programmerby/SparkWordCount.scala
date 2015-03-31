package programmerby

import org.apache.spark._

object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <master> <input> [<output>]")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName(this.getClass.getSimpleName)

    val spark = new SparkContext(conf)

    val input = spark.textFile(args(1))

    val counts = input.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    if (args.length > 2) {
      counts.saveAsTextFile(args(2))
    } else {
      val output = counts.collect()
      for (tuple <- output) {
        println(tuple._1 + ": " + tuple._2)
      }
    }
  }
}
