package com.training

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
  * Test fixture that creates a single shared SparkSession for all tests.
  * Keeps transformations as pure functions so they can be unit tested
  * with small in-memory DataFrames.
  */
trait TestSparkSession { self: AnyFunSuite =>

  protected var spark: SparkSession = _

  def beforeSpark(): Unit = {
    WindowsHadoopSupport.configure()
    spark = SparkSession.builder()
      .appName("Employee Tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  def afterSpark(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      spark = null
    }
  }
}

// =====================================================================
//  Step 1 – Basic function tests
// =====================================================================

class BasicFunctionTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("increaseSalary adds 5000 to every salary") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.increaseSalary(df)
    val salaries = result.select("salary").as[Int].collect().toSeq
    salaries shouldBe Seq(35000, 45000)
  }

  test("increaseSalaryBy adds arbitrary amount") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.increaseSalaryBy(df, 10000)
    result.collect().head.getAs[Int]("salary") shouldBe 40000
  }

  test("removeDuplicates removes fully duplicate rows") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.removeDuplicates(df)
    result.count() shouldBe 2
  }

  test("removeDuplicatesBy removes duplicates based on selected columns") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (1, "Jane", 50000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.removeDuplicatesBy(df, "id")
    result.count() shouldBe 2
  }
}

// =====================================================================
//  Step 2 – Closure tests
// =====================================================================

class ClosureTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("filterByMinSalary returns a closure that filters correctly") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 50000),
      (3, "Smith", 20000)
    ).toDF("id", "name", "salary")

    val filterFn = EmployeeTransformations.filterByMinSalary(35000)
    val result = filterFn(df)
    result.count() shouldBe 1
    result.collect().head.getAs[String]("name") shouldBe "David"
  }

  test("scaleSalary returns a closure that multiplies salary") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000)
    ).toDF("id", "name", "salary")

    val scaleFn = EmployeeTransformations.scaleSalary(1.1)
    val result = scaleFn(df)
    result.collect().head.getAs[Double]("salary") shouldBe 33000.0
  }

  test("filterColumn closure with greater-than operator") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 50000)
    ).toDF("id", "name", "salary")

    val filterFn = EmployeeTransformations.filterColumn("salary", ">", 40000)
    val result = filterFn(df)
    result.count() shouldBe 1
  }

  test("filterColumn closure with equals operator") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 50000)
    ).toDF("id", "name", "salary")

    val filterFn = EmployeeTransformations.filterColumn("name", "=", "John")
    val result = filterFn(df)
    result.count() shouldBe 1
  }

  test("filterColumn closure with less-than-or-equal operator") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 50000),
      (3, "Smith", 20000)
    ).toDF("id", "name", "salary")

    val filterFn = EmployeeTransformations.filterColumn("salary", "<=", 30000)
    val result = filterFn(df)
    result.count() shouldBe 2
  }

  test("closures can be composed") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 50000),
      (3, "Smith", 20000)
    ).toDF("id", "name", "salary")

    val filterFn = EmployeeTransformations.filterByMinSalary(25000)
    val scaleFn  = EmployeeTransformations.scaleSalary(2.0)
    val result = scaleFn(filterFn(df))
    result.count() shouldBe 2
    val salaries = result.select("salary").as[Double].collect().toSeq
    salaries should contain only (60000.0, 100000.0)
  }
}

// =====================================================================
//  Step 3 – DataFrame operation tests
// =====================================================================

class DataFrameOperationTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("selectColumns returns only the requested columns") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.selectColumns(df, "name", "salary")
    result.columns.toSeq shouldBe Seq("name", "salary")
  }

  test("renameColumn renames a column") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.renameColumn(df, "name", "employee_name")
    result.columns.toSeq shouldBe Seq("id", "employee_name", "salary")
  }

  test("addBonusColumn adds a computed bonus column") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.addBonusColumn(df, 0.1)
    result.columns should contain("bonus")
    result.collect().head.getAs[Double]("bonus") shouldBe 3000.0
  }

  test("sortByColumn sorts ascending by default") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 50000),
      (2, "David", 30000),
      (3, "Smith", 40000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.sortByColumn(df, "salary", ascending = true)
    val salaries = result.select("salary").as[Int].collect().toSeq
    salaries shouldBe Seq(30000, 40000, 50000)
  }

  test("sortByColumn sorts descending") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 50000),
      (2, "David", 30000),
      (3, "Smith", 40000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.sortByColumn(df, "salary", ascending = false)
    val salaries = result.select("salary").as[Int].collect().toSeq
    salaries shouldBe Seq(50000, 40000, 30000)
  }

  test("dropNulls removes rows with null in specified columns") {
    import spark.implicits._
    val df = Seq(
      (1, Some("John"), 30000),
      (2, None, 40000),
      (3, Some("Smith"), 50000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.dropNulls(df, "name")
    result.count() shouldBe 2
  }

  test("dropNullsOrEmpty removes null and empty-string rows") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "", 40000),
      (3, "Smith", 50000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.dropNullsOrEmpty(df, "name")
    result.count() shouldBe 2
  }
}

// =====================================================================
//  Step 4 – Aggregation tests
// =====================================================================

class AggregationTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("salaryStats computes correct statistics") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000),
      (3, "Smith", 50000)
    ).toDF("id", "name", "salary")

    val stats = EmployeeTransformations.salaryStats(df)
    stats.collect().head.getAs[Double]("avg_salary") shouldBe 40000.0
    stats.collect().head.getAs[Int]("min_salary") shouldBe 30000
    stats.collect().head.getAs[Int]("max_salary") shouldBe 50000
    stats.collect().head.getAs[Long]("total_salary") shouldBe 120000L
    stats.collect().head.getAs[Long]("employee_count") shouldBe 3L
  }

  test("avgSalaryByGroup groups and averages correctly") {
    import spark.implicits._
    val df = Seq(
      (1, "John", "Engineering", 30000),
      (2, "David", "Engineering", 40000),
      (3, "Smith", "Sales", 50000)
    ).toDF("id", "name", "department", "salary")

    val result = EmployeeTransformations.avgSalaryByGroup(df, "department")
    val engAvg = result.filter(col("department") === "Engineering")
      .collect().head.getAs[Double]("avg_salary")
    engAvg shouldBe 35000.0
  }
}

// =====================================================================
//  Step 5 – Data quality assertion tests
// =====================================================================

class DataQualityTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  // --- Row counts ---

  test("row count is preserved through a no-op transformation") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val beforeCount = df.count()
    val result = EmployeeTransformations.increaseSalaryBy(df, 0)
    val afterCount = result.count()

    afterCount shouldBe beforeCount
  }

  test("row count decreases after removeDuplicates") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val beforeCount = df.count()
    val result = EmployeeTransformations.removeDuplicates(df)
    val afterCount = result.count()

    afterCount shouldBe 2
    afterCount should be < beforeCount
  }

  test("row count decreases after filterByMinSalary") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 50000),
      (3, "Smith", 20000)
    ).toDF("id", "name", "salary")

    val filterFn = EmployeeTransformations.filterByMinSalary(35000)
    val result = filterFn(df)
    result.count() shouldBe 1
  }

  // --- Null rate checks ---

  test("nullCountMap returns correct null counts") {
    import spark.implicits._
    val df = Seq(
      (1, Some("John"), 30000),
      (2, None, 40000),
      (3, Some("Smith"), null)
    ).toDF("id", "name", "salary")

    val nullCounts = EmployeeTransformations.nullCountMap(df)
    nullCounts("name") shouldBe 1L
    nullCounts("salary") shouldBe 1L
    nullCounts("id") shouldBe 0L
  }

  test("nullRateMap returns correct null rates") {
    import spark.implicits._
    val df = Seq(
      (1, Some("John"), 30000),
      (2, None, 40000),
      (3, Some("Smith"), 50000)
    ).toDF("id", "name", "salary")

    val rates = EmployeeTransformations.nullRateMap(df)
    rates("name") shouldBe (1.0 / 3.0)
    rates("id") shouldBe 0.0
  }

  test("nullRateMap handles empty DataFrame") {
    import spark.implicits._
    val df = Seq.empty[(Int, String, Int)].toDF("id", "name", "salary")

    val rates = EmployeeTransformations.nullRateMap(df)
    rates("id") shouldBe 0.0
    rates("name") shouldBe 0.0
  }

  test("nullCounts DataFrame returns one row with counts per column") {
    import spark.implicits._
    val df = Seq(
      (1, Some("John"), 30000),
      (2, None, 40000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.nullCounts(df)
    result.count() shouldBe 1
    val row = result.head()
    row.getLong(1) shouldBe 1L // name has 1 null
  }

  // --- Required column null checks ---

  test("required columns have no nulls when data is clean") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val nullCounts = EmployeeTransformations.nullCountMap(df)
    nullCounts("id") shouldBe 0L
    nullCounts("name") shouldBe 0L
    nullCounts("salary") shouldBe 0L
  }

  test("null rate threshold check — flag columns exceeding threshold") {
    import spark.implicits._
    val df = Seq(
      (1, Some("John"), 30000),
      (2, None, 40000),
      (3, None, 50000)
    ).toDF("id", "name", "salary")

    val rates = EmployeeTransformations.nullRateMap(df)
    val threshold = 0.1
    val flagged = rates.filter { case (_, rate) => rate > threshold }
    flagged should contain key "name"
    flagged("name") shouldBe (2.0 / 3.0)
  }

  // --- Uniqueness checks ---

  test("isUnique returns true for unique business key") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000),
      (3, "Smith", 50000)
    ).toDF("id", "name", "salary")

    EmployeeTransformations.isUnique(df, "id") shouldBe true
  }

  test("isUnique returns false when duplicates exist") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (1, "Jane", 40000),
      (3, "Smith", 50000)
    ).toDF("id", "name", "salary")

    EmployeeTransformations.isUnique(df, "id") shouldBe false
  }

  test("distinctCount returns correct count") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000),
      (3, "John", 50000)
    ).toDF("id", "name", "salary")

    EmployeeTransformations.distinctCount(df, "name") shouldBe 2L
  }
}

// =====================================================================
//  Step 6 – Batch processing end-to-end tests
// =====================================================================

class BatchProcessingTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("full batch pipeline: load -> filter -> transform -> deduplicate -> aggregate") {
    import spark.implicits._

    // Simulate raw data load
    val raw = Seq(
      (1, "John", "Engineering", 30000),
      (2, "David", "Engineering", 40000),
      (3, "Smith", "Sales", 50000),
      (4, "Jane", "Sales", 20000),
      (1, "John", "Engineering", 30000) // duplicate
    ).toDF("id", "name", "department", "salary")

    val beforeCount = raw.count()
    beforeCount shouldBe 5

    // Step 1: remove duplicates
    val deduped = EmployeeTransformations.removeDuplicates(raw)
    deduped.count() shouldBe 4

    // Step 2: filter by min salary (closure)
    val filterFn = EmployeeTransformations.filterByMinSalary(25000)
    val filtered = filterFn(deduped)
    filtered.count() shouldBe 3

    // Step 3: increase salary
    val increased = EmployeeTransformations.increaseSalary(filtered)
    val salaries = increased.select("salary").as[Int].collect().toSeq
    salaries should contain only (35000, 45000, 55000)

    // Step 4: add bonus column
    val withBonus = EmployeeTransformations.addBonusColumn(increased, 0.1)
    withBonus.columns should contain("bonus")

    // Step 5: aggregate
    val stats = EmployeeTransformations.salaryStats(withBonus)
    stats.collect().head.getAs[Long]("employee_count") shouldBe 3L

    // Data quality: id uniqueness
    EmployeeTransformations.isUnique(filtered, "id") shouldBe true
  }

  test("batch pipeline with department grouping") {
    import spark.implicits._

    val df = Seq(
      (1, "John", "Engineering", 30000),
      (2, "David", "Engineering", 40000),
      (3, "Smith", "Sales", 50000),
      (4, "Jane", "Sales", 20000)
    ).toDF("id", "name", "department", "salary")

    val grouped = EmployeeTransformations.avgSalaryByGroup(df, "department")
    grouped.count() shouldBe 2

    val engAvg = grouped.filter(col("department") === "Engineering")
      .collect().head.getAs[Double]("avg_salary")
    engAvg shouldBe 35000.0

    val salesAvg = grouped.filter(col("department") === "Sales")
      .collect().head.getAs[Double]("avg_salary")
    salesAvg shouldBe 35000.0
  }

  test("pipeline handles null data gracefully") {
    import spark.implicits._

    val df = Seq(
      (1, Some("John"), Some(30000)),
      (2, None, Some(40000)),
      (3, Some("Smith"), None)
    ).toDF("id", "name", "salary")

    // Drop nulls in required columns
    val cleaned = EmployeeTransformations.dropNulls(df, "name", "salary")
    cleaned.count() shouldBe 1
    cleaned.collect().head.getAs[String]("name") shouldBe "John"
  }
}

// =====================================================================
//  Step 7 & 8 – Streaming tests with in-memory sources
// =====================================================================

class StreamingTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("streaming: read from rate source and apply transformation") {
    import spark.implicits._

    val checkpointDir = java.io.File.createTempFile("spark-checkpoint-", "").getParentFile
    checkpointDir.delete()

    try {
      // Create a streaming DataFrame using the rate source (1 partition, 1 row/sec)
      val stream = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .option("numPartitions", "1")
        .load()

      // Apply a pure transformation
      val transformed = stream.withColumn("doubled_value", col("value") * 2)

      val query = transformed.writeStream
        .format("memory")
        .queryName("streaming_test")
        .outputMode("append")
        .start()

      try {
        // Wait for some data to arrive
        Thread.sleep(2000)
        query.processAvailable()

        val result = spark.table("streaming_test")
        result.count() should be > 0L

        // Verify the transformation is correct
        val rows = result.collect()
        rows.foreach { row =>
          val original = row.getAs[Long]("value")
          val doubled  = row.getAs[Long]("doubled_value")
          doubled shouldBe (original * 2)
        }
      } finally {
        query.stop()
        query.awaitTermination()
      }
    } finally {
      // Cleanup checkpoint directory
      def deleteRecursively(f: java.io.File): Unit = {
        if (f.isDirectory) f.listFiles.foreach(deleteRecursively)
        f.delete()
      }
      deleteRecursively(checkpointDir)
    }
  }

  test("streaming: multiple batches verify cumulative output") {
    import spark.implicits._

    val checkpointDir = java.io.File.createTempFile("spark-checkpoint-", "").getParentFile
    checkpointDir.delete()

    try {
      val stream = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .option("numPartitions", "1")
        .load()

      val transformed = stream.withColumn("salary", col("value") + 30000)

      val query = transformed.writeStream
        .format("memory")
        .queryName("streaming_cumulative")
        .outputMode("append")
        .start()

      try {
        // First batch
        Thread.sleep(1500)
        query.processAvailable()
        val countAfterFirst = spark.table("streaming_cumulative").count()
        countAfterFirst should be > 0L

        // Second batch — more data should accumulate
        Thread.sleep(1500)
        query.processAvailable()
        val countAfterSecond = spark.table("streaming_cumulative").count()
        countAfterSecond should be >= countAfterFirst

      } finally {
        query.stop()
        query.awaitTermination()
      }
    } finally {
      def deleteRecursively(f: java.io.File): Unit = {
        if (f.isDirectory) f.listFiles.foreach(deleteRecursively)
        f.delete()
      }
      deleteRecursively(checkpointDir)
    }
  }

  test("streaming: apply filterByMinSalary closure on stream") {
    import spark.implicits._

    val checkpointDir = java.io.File.createTempFile("spark-checkpoint-", "").getParentFile
    checkpointDir.delete()

    try {
      val stream = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .option("numPartitions", "1")
        .load()

      // Map the "value" column to "salary" for the closure
      val withSalary = stream.withColumn("salary", col("value") * 10000)

      // Apply the pure closure transformation
      val filterFn = EmployeeTransformations.filterByMinSalary(50000)
      val filtered = filterFn(withSalary)

      val query = filtered.writeStream
        .format("memory")
        .queryName("streaming_filter")
        .outputMode("append")
        .start()

      try {
        Thread.sleep(2000)
        query.processAvailable()

        val result = spark.table("streaming_filter")
        // All rows should have salary >= 50000
        val lowSalaryCount = result.filter(col("salary") < 50000).count()
        lowSalaryCount shouldBe 0L
      } finally {
        query.stop()
        query.awaitTermination()
      }
    } finally {
      def deleteRecursively(f: java.io.File): Unit = {
        if (f.isDirectory) f.listFiles.foreach(deleteRecursively)
        f.delete()
      }
      deleteRecursively(checkpointDir)
    }
  }

  test("streaming: query is stopped in finally block") {
    import spark.implicits._

    val checkpointDir = java.io.File.createTempFile("spark-checkpoint-", "").getParentFile
    checkpointDir.delete()

    try {
      val stream = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .option("numPartitions", "1")
        .load()

      val query = stream.writeStream
        .format("memory")
        .queryName("streaming_finally")
        .outputMode("append")
        .start()

      var stopped = false
      try {
        Thread.sleep(1000)
        query.processAvailable()
        spark.table("streaming_finally").count() should be >= 0L
      } finally {
        query.stop()
        query.awaitTermination()
        stopped = true
      }

      stopped shouldBe true
    } finally {
      def deleteRecursively(f: java.io.File): Unit = {
        if (f.isDirectory) f.listFiles.foreach(deleteRecursively)
        f.delete()
      }
      deleteRecursively(checkpointDir)
    }
  }
}

// =====================================================================
//  Step 9 – Comprehensive checklist validation
// =====================================================================

class ChecklistValidationTest extends AnyFunSuite with TestSparkSession with Matchers {

  override def beforeAll(): Unit = {
    beforeSpark()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      afterSpark()
    }
  }

  test("CHECKLIST: transformations are pure functions — no side effects") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    // Running the same transformation twice should produce the same result
    val r1 = EmployeeTransformations.increaseSalary(df)
    val r2 = EmployeeTransformations.increaseSalary(df)
    r1.collect().toSeq shouldBe r2.collect().toSeq
  }

  test("CHECKLIST: row counts checked before and after important transformations") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000),
      (3, "Smith", 50000)
    ).toDF("id", "name", "salary")

    val beforeCount = df.count()
    val result = EmployeeTransformations.increaseSalary(df)
    val afterCount = result.count()
    beforeCount shouldBe afterCount
    beforeCount shouldBe 3L
  }

  test("CHECKLIST: required columns checked for null values") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000)
    ).toDF("id", "name", "salary")

    val nullCounts = EmployeeTransformations.nullCountMap(df)
    nullCounts("id") shouldBe 0L
    nullCounts("name") shouldBe 0L
    nullCounts("salary") shouldBe 0L
  }

  test("CHECKLIST: business keys checked for uniqueness") {
    import spark.implicits._
    val df = Seq(
      (1, "John", 30000),
      (2, "David", 40000),
      (3, "Smith", 50000)
    ).toDF("id", "name", "salary")

    EmployeeTransformations.isUnique(df, "id") shouldBe true
  }

  test("CHECKLIST: tests use deterministic input — no external dependencies") {
    import spark.implicits._
    // All data is created inline — no Kafka, no cloud storage, no production data
    val df = Seq(
      (1, "John", 30000)
    ).toDF("id", "name", "salary")

    val result = EmployeeTransformations.increaseSalary(df)
    result.collect().head.getAs[Int]("salary") shouldBe 35000
  }

  test("CHECKLIST: full pipeline with all quality gates") {
    import spark.implicits._

    // Deterministic input
    val raw = Seq(
      (1, "John", "Engineering", 30000),
      (2, "David", "Engineering", 40000),
      (3, "Smith", "Sales", 50000),
      (4, "Jane", "Sales", 20000)
    ).toDF("id", "name", "department", "salary")

    // Row count before
    val beforeCount = raw.count()
    beforeCount shouldBe 4L

    // Null rate check
    val nullRates = EmployeeTransformations.nullRateMap(raw)
    nullRates.values.foreach { rate =>
      rate shouldBe 0.0
    }

    // Uniqueness check
    EmployeeTransformations.isUnique(raw, "id") shouldBe true

    // Transform
    val deduped = EmployeeTransformations.removeDuplicates(raw)
    val filterFn = EmployeeTransformations.filterByMinSalary(25000)
    val filtered = filterFn(deduped)
    val increased = EmployeeTransformations.increaseSalary(filtered)

    // Row count after
    val afterCount = increased.count()
    afterCount shouldBe 3L

    // Final quality check
    EmployeeTransformations.isUnique(filtered, "id") shouldBe true
  }
}
