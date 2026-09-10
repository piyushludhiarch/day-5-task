# Day 5 — Transformations and Actions

## Scala + Apache Spark — 30-Day Practice

This project is part of my **30-Day Scala + Apache Spark Practice** series.

On **Day 5**, I practiced the core concept of **Transformations and Actions in Apache Spark**, with hands-on examples using Scala and Spark RDDs.

The main goal was to understand how Spark processes data lazily through transformations and produces results only when an action is executed.

---

## 📚 Topics Covered

### Transformations

I practiced the following Spark transformations:

* `map`
* `filter`
* `flatMap`
* `distinct`
* `union`

### Actions

I practiced the following Spark actions:

* `count`
* `collect`
* `first`
* `take`
* `reduce`

### Core Concepts

* Difference between **Transformations and Actions**
* Spark's **Lazy Evaluation**
* Which operations are lazy
* How Spark builds a **DAG (Directed Acyclic Graph)**
* How actions trigger Spark computation
* Practical log analysis using Spark

---

# 🔄 Transformations vs Actions

## Transformations

Transformations are operations that create a **new RDD from an existing RDD**.

They do not immediately execute the computation.

Examples:

```scala
val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

val doubled = numbers.map(x => x * 2)

val evenNumbers = numbers.filter(x => x % 2 == 0)
```

The above operations are **lazy**.

Spark remembers what needs to be done but does not execute it immediately.

### Common Transformations

| Transformation | Purpose                                 |
| -------------- | --------------------------------------- |
| `map()`        | Applies a function to every element     |
| `filter()`     | Keeps elements that satisfy a condition |
| `flatMap()`    | Maps elements and flattens the result   |
| `distinct()`   | Removes duplicate elements              |
| `union()`      | Combines two RDDs                       |

---

# ⚡ Actions

Actions are operations that **trigger the execution of Spark transformations** and return a result.

Examples:

```scala
numbers.count()
numbers.collect()
numbers.first()
numbers.take(3)
numbers.reduce((a, b) => a + b)
```

### Common Actions

| Action      | Purpose                            |
| ----------- | ---------------------------------- |
| `count()`   | Returns the number of elements     |
| `collect()` | Returns all elements to the driver |
| `first()`   | Returns the first element          |
| `take(n)`   | Returns the first `n` elements     |
| `reduce()`  | Combines elements using a function |

---

# 💤 Lazy Evaluation

One of the most important concepts practiced today was **lazy evaluation**.

Spark does not execute a transformation immediately.

For example:

```scala
val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

val result = numbers
  .filter(x => x % 2 == 0)
  .map(x => x * 10)
```

At this point, Spark has **not necessarily executed the computation**.

When an action is called:

```scala
result.collect()
```

Spark executes the required transformations.

### Simple Flow

```text
RDD
 │
 ▼
filter()
 │
 ▼
map()
 │
 ▼
collect()
 │
 ▼
Spark executes the computation
 │
 ▼
Result
```

This lazy execution allows Spark to optimize how the computation is performed.

---

# 🧪 Transformation Examples

## map()

`map()` applies a function to every element.

```scala
val numbers = sc.parallelize(List(1, 2, 3, 4))

val result = numbers.map(x => x * 2)

println(result.collect().mkString(", "))
```

Output:

```text
2, 4, 6, 8
```

---

## filter()

`filter()` keeps only elements that satisfy a condition.

```scala
val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

val evenNumbers = numbers.filter(x => x % 2 == 0)

println(evenNumbers.collect().mkString(", "))
```

Output:

```text
2, 4
```

---

## flatMap()

`flatMap()` maps each element and then flattens the resulting collections.

```scala
val sentences = sc.parallelize(
  List("Hello Spark", "Hello Scala")
)

val words = sentences.flatMap(_.split(" "))

println(words.collect().mkString(", "))
```

Output:

```text
Hello, Spark, Hello, Scala
```

---

## distinct()

`distinct()` removes duplicate values.

```scala
val numbers = sc.parallelize(List(1, 2, 2, 3, 3, 4))

val uniqueNumbers = numbers.distinct()

println(uniqueNumbers.collect().mkString(", "))
```

Output:

```text
1, 2, 3, 4
```

---

## union()

`union()` combines two RDDs.

```scala
val rdd1 = sc.parallelize(List(1, 2, 3))
val rdd2 = sc.parallelize(List(4, 5, 6))

val combined = rdd1.union(rdd2)

println(combined.collect().mkString(", "))
```

Output:

```text
1, 2, 3, 4, 5, 6
```

---

# 🎯 Action Examples

## count()

Returns the number of elements.

```scala
val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

println(numbers.count())
```

Output:

```text
5
```

---

## collect()

Returns all elements of the RDD to the driver.

```scala
println(numbers.collect().mkString(", "))
```

Output:

```text
1, 2, 3, 4, 5
```

> `collect()` should be used carefully with large datasets because all data is brought to the driver.

---

## first()

Returns the first element.

```scala
println(numbers.first())
```

Output:

```text
1
```

---

## take()

Returns the first `n` elements.

```scala
println(numbers.take(3).mkString(", "))
```

Output:

```text
1, 2, 3
```

---

## reduce()

`reduce()` combines the elements of an RDD using a function.

```scala
val numbers = sc.parallelize(List(1, 2, 3, 4))

val sum = numbers.reduce((a, b) => a + b)

println(sum)
```

Output:

```text
10
```

---

# 📊 Scenario — Log Analyzer

The main practical scenario for Day 5 was to build a simple **log analyzer that counts ERROR messages**.

A sample log file contains entries such as:

```text
INFO Application started
INFO User logged in
ERROR Database connection failed
INFO Processing request
ERROR File not found
ERROR Database connection failed
INFO Application stopped
```

The objective is to find how many log entries contain `ERROR`.

### Basic Spark Approach

```scala
val logs = sc.textFile("data/app.log")

val errorLogs = logs.filter(line => line.contains("ERROR"))

val errorCount = errorLogs.count()

println(s"Total ERROR messages: $errorCount")
```

### Processing Flow

```text
Log File
   │
   ▼
textFile()
   │
   ▼
RDD[String]
   │
   ▼
filter(line => line.contains("ERROR"))
   │
   ▼
ERROR Logs
   │
   ▼
count()
   │
   ▼
Number of ERROR messages
```

Here:

* `textFile()` creates an RDD.
* `filter()` is a **transformation**.
* `count()` is an **action**.
* `filter()` is lazy.
* `count()` triggers the actual computation.

---

# 🧠 What I Learned

By completing Day 5, I learned:

1. Transformations create new RDDs.
2. Actions trigger Spark computation.
3. Transformations are generally **lazy**.
4. Actions return results or write output.
5. Spark uses lazy evaluation to optimize execution.
6. `map`, `filter`, `flatMap`, `distinct`, and `union` are transformations.
7. `count`, `collect`, `first`, `take`, and `reduce` are actions.
8. Log files can be processed efficiently using Spark transformations and actions.
9. `collect()` should be used carefully because it brings data to the driver.
10. A simple Spark pipeline can be used to analyze application logs and identify errors.

---

# 📁 Project Structure

```text
day5-transformations-actions/
│
├── data/
│   └── app.log
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           └── Day5Assignment.scala
│
├── build.sbt
├── .gitignore
└── README.md
```

---

# 🛠️ Technologies Used

* **Scala**
* **Apache Spark**
* **SBT**
* **Ubuntu / Linux**
* **Git**
* **GitHub**

---

# ▶️ How to Run

Clone the repository:

```bash
git clone https://github.com/piyushludhiarch/day-5-task.git
```

Move into the project:

```bash
cd day-5-task
```

Run the Scala/Spark project using SBT:

```bash
sbt run
```

---

# 📌 Day 5 Practice Checklist

* [x] Practice `map`
* [x] Practice `filter`
* [x] Practice `flatMap`
* [x] Practice `distinct`
* [x] Practice `union`
* [x] Practice `count`
* [x] Practice `collect`
* [x] Practice `first`
* [x] Practice `take`
* [x] Practice `reduce`
* [x] Understand transformations vs actions
* [x] Understand lazy evaluation
* [x] Build a basic ERROR log analyzer

---

## 🚀 30-Day Scala + Spark Practice

**Day 5 completed — Transformations and Actions**

This exercise strengthened my understanding of Spark's RDD programming model, lazy evaluation, transformations, actions, and basic real-world log processing.
