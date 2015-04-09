package programmerby

import org.apache.spark._

object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <input> [<output>]")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.setMaster(conf.get("spark.master", "local"))
      .setAppName(this.getClass.getSimpleName)

    val spark = new SparkContext(conf)

    val input = spark.textFile(args(0))

    val counts = input.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    if (args.length > 1) {
      counts.saveAsTextFile(args(1))
    } else {
      val output = counts.collect()
      for (tuple <- output) {
        println(tuple._1 + ": " + tuple._2)
      }
    }
  }
}
