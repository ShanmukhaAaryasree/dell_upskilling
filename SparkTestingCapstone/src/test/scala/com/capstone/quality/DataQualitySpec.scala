package com.capstone.quality

import com.capstone.fixture.SparkSessionFixture
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Step 5: Data quality assertion tests. Tests row counts, null rates, uniqueness, and column
  * existence checks.
  */
class DataQualitySpec extends AnyFlatSpec with Matchers with SparkSessionFixture {

  val spark: org.apache.spark.sql.SparkSession = buildSparkSession()
  import spark.implicits._

  // --- checkRowCount ---

  "checkRowCount" should "pass when row count matches expected" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    val result = DataQuality.checkRowCount(data, 3, "exact count")
    result.passed should be(true)
    result.checkName should be("exact count")
  }

  it should "fail when row count does not match" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    val result = DataQuality.checkRowCount(data, 5, "wrong count")
    result.passed should be(false)
    result.message should include("5")
    result.message should include("2")
  }

  // --- checkRowCountRange ---

  "checkRowCountRange" should "pass when count is within range" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    val result = DataQuality.checkRowCountRange(data, 2, 5, "in range")
    result.passed should be(true)
  }

  it should "fail when count is below range" in {
    val data = Seq(
      (1, "Alice")
    ).toDF("id", "name")

    val result = DataQuality.checkRowCountRange(data, 2, 5, "below range")
    result.passed should be(false)
  }

  it should "fail when count is above range" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie"),
      (4, "Diana"),
      (5, "Eve"),
      (6, "Frank")
    ).toDF("id", "name")

    val result = DataQuality.checkRowCountRange(data, 2, 5, "above range")
    result.passed should be(false)
  }

  // --- checkNoNulls ---

  "checkNoNulls" should "pass when column has no nulls" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    val result = DataQuality.checkNoNulls(data, "name")
    result.passed should be(true)
  }

  it should "fail when column has nulls" in {
    val data = Seq(
      (1, Some("Alice")),
      (2, None),
      (3, Some("Charlie"))
    ).toDF("id", "name")

    val result = DataQuality.checkNoNulls(data, "name")
    result.passed should be(false)
    result.message should include("1")
  }

  // --- checkNullRate ---

  "checkNullRate" should "pass when null rate is below threshold" in {
    val data = Seq(
      (1, Some("Alice")),
      (2, Some("Bob")),
      (3, None)
    ).toDF("id", "name")

    // 1 out of 3 = 33.3% null rate, threshold 50%
    val result = DataQuality.checkNullRate(data, "name", 0.5)
    result.passed should be(true)
  }

  it should "fail when null rate exceeds threshold" in {
    val data = Seq(
      (1, Some("Alice")),
      (2, None),
      (3, None)
    ).toDF("id", "name")

    // 2 out of 3 = 66.7% null rate, threshold 10%
    val result = DataQuality.checkNullRate(data, "name", 0.1)
    result.passed should be(false)
  }

  it should "pass for empty DataFrame" in {
    val schema = org.apache.spark.sql.types.StructType(
      Seq(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType)
      )
    )
    val data = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)

    val result = DataQuality.checkNullRate(data, "name", 0.1)
    result.passed should be(true)
  }

  // --- checkUniqueness ---

  "checkUniqueness" should "pass when all values are unique" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    val result = DataQuality.checkUniqueness(data, "id")
    result.passed should be(true)
  }

  it should "fail when duplicates exist" in {
    val data = Seq(
      (1, "Alice"),
      (1, "Bob"),
      (2, "Charlie")
    ).toDF("id", "name")

    val result = DataQuality.checkUniqueness(data, "id")
    result.passed should be(false)
  }

  // --- checkColumnsExist ---

  "checkColumnsExist" should "pass when all required columns exist" in {
    val data = Seq(
      (1, "Alice", 30)
    ).toDF("id", "name", "age")

    val result = DataQuality.checkColumnsExist(data, Seq("id", "name", "age"))
    result.passed should be(true)
  }

  it should "fail when columns are missing" in {
    val data = Seq(
      (1, "Alice", 30)
    ).toDF("id", "name", "age")

    val result = DataQuality.checkColumnsExist(data, Seq("id", "name", "salary"))
    result.passed should be(false)
    result.message should include("salary")
  }

  // --- runAllChecks ---

  "runAllChecks" should "aggregate multiple check results" in {
    val data = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    val checks = Seq[() => DataQuality.QualityCheckResult](
      () => DataQuality.checkRowCount(data, 2, "count"),
      () => DataQuality.checkNoNulls(data, "name", "no nulls"),
      () => DataQuality.checkUniqueness(data, "id", "unique id")
    )

    val report = DataQuality.runAllChecks(data, checks)
    report.allPassed should be(true)
    report.results.size should be(3)
    report.failed should be(empty)
  }

  it should "report failures correctly" in {
    val data = Seq(
      (1, Some("Alice")),
      (1, None)
    ).toDF("id", "name")

    val checks = Seq[() => DataQuality.QualityCheckResult](
      () => DataQuality.checkRowCount(data, 2, "count"),
      () => DataQuality.checkNoNulls(data, "name", "no nulls"),
      () => DataQuality.checkUniqueness(data, "id", "unique id")
    )

    val report = DataQuality.runAllChecks(data, checks)
    report.allPassed should be(false)
    report.failed.size should be > 0
  }

  // --- validateSalesData ---

  "validateSalesData" should "pass for valid sales data" in {
    val data = Seq(
      (1, "Alice", "Electronics", 2, 29.99),
      (2, "Bob", "Books", 1, 15.50),
      (3, "Charlie", "Clothing", 3, 12.00)
    ).toDF("id", "name", "category", "quantity", "unit_price")

    val report = DataQuality.validateSalesData(
      data,
      expectedMinRows = 1,
      expectedMaxRows = 100,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name")
    )

    report.allPassed should be(true)
  }

  it should "fail for data with duplicate keys" in {
    val data = Seq(
      (1, "Alice", "Electronics", 2, 29.99),
      (1, "Bob", "Books", 1, 15.50)
    ).toDF("id", "name", "category", "quantity", "unit_price")

    val report = DataQuality.validateSalesData(
      data,
      expectedMinRows = 1,
      expectedMaxRows = 100,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name")
    )

    report.allPassed should be(false)
    report.failed.exists(_.checkName.contains("unique")) should be(true)
  }
}
