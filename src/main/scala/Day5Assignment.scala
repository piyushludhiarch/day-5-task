import org.apache.spark.{SparkConf, SparkContext}

object Day5Assignment {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day5TransformationsActions")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5))

      println("=== Transformations ===")

      val squared = numbers.map(n => n * n)
      println(s"map: ${squared.collect().mkString(", ")}")

      val evenNumbers = numbers.filter(n => n % 2 == 0)
      println(s"filter: ${evenNumbers.collect().mkString(", ")}")

      val expanded = numbers.flatMap(n => Seq(n, n * 10))
      println(s"flatMap: ${expanded.collect().mkString(", ")}")

      val firstSet = sc.parallelize(Seq(1, 2, 3, 4, 5))
      val secondSet = sc.parallelize(Seq(4, 5, 6, 7, 8))

      val combined = firstSet.union(secondSet)
      println(s"union: ${combined.collect().mkString(", ")}")

      val unique = combined.distinct()
      println(s"distinct: ${unique.collect().sorted.mkString(", ")}")

      println()
      println("=== Actions ===")

      println(s"count: ${numbers.count()}")
      println(s"collect: ${numbers.collect().mkString(", ")}")
      println(s"first: ${numbers.first()}")
      println(s"take(3): ${numbers.take(3).mkString(", ")}")
      println(s"reduce: ${numbers.reduce((a, b) => a + b)}")

      println()
      println("=== Log Analyzer ===")

      val logs = sc.textFile("data/app.log")
      val errorLogs = logs.filter(line => line.startsWith("ERROR"))

      val errorCount = errorLogs.count()

      println(s"ERROR count: $errorCount")
      println("ERROR messages:")

      errorLogs.collect().foreach(println)

    } finally {
      sc.stop()
    }
  }
}
