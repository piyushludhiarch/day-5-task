import org.apache.spark.sql.SparkSession

object Day5Assignment {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day5Assignment")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val nums1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val nums2 = sc.parallelize(List(4, 5, 6, 7, 8))

    println("=== map: square each number ===")
    val squared = nums1.map(x => x * x)
    println(squared.collect().mkString(", "))

    println("\n=== filter: even numbers only ===")
    val evens = nums1.filter(x => x % 2 == 0)
    println(evens.collect().mkString(", "))

    println("\n=== flatMap: expand each number into a range ===")
    val flat = nums1.flatMap(x => List(x, x * 10))
    println(flat.collect().mkString(", "))

    println("\n=== distinct: remove duplicates from union ===")
    val combined = nums1.union(nums2)
    println("Union: " + combined.collect().mkString(", "))
    println("Distinct: " + combined.distinct().collect().mkString(", "))

    println("\n=== count ===")
    println(s"Count of nums1: ${nums1.count()}")

    println("\n=== collect ===")
    println(nums1.collect().mkString(", "))

    println("\n=== first ===")
    println(nums1.first())

    println("\n=== take(3) ===")
    println(nums1.take(3).mkString(", "))

    println("\n=== reduce: sum all numbers ===")
    println(nums1.reduce((a, b) => a + b))

    println("""
      |=== Transformations vs Actions ===
      |Transformations (map, filter, flatMap, distinct, union) build a new RDD from an
      |existing one but do NOT execute immediately. Spark records them as a lazy DAG
      |(a plan) and waits until an action is called.
      |
      |Actions (count, collect, first, take, reduce) trigger actual computation. When an
      |action is called, Spark executes the DAG of transformations built so far and
      |returns a result to the driver program (or writes output).
      |
      |Why laziness matters: Spark can optimize the whole chain of transformations before
      |running anything, combine steps, and avoid unnecessary computation - it only runs
      |what's needed to produce the result requested by an action.
      |
      |Lazy operations: map, filter, flatMap, distinct, union
      |Eager operations (actions): count, collect, first, take, reduce
      |""".stripMargin)

    println("=== Scenario: Log Analyzer - counting ERROR messages ===")
    val logs = sc.textFile("data/app.log")
    val errorLines = logs.filter(line => line.contains("ERROR"))
    val errorCount = errorLines.count()
    println(s"Total ERROR messages: $errorCount")
    println("ERROR lines:")
    errorLines.collect().foreach(println)

    spark.stop()
  }
}
