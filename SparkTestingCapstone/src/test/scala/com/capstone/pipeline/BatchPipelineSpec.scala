package com.capstone.pipeline

import com.capstone.fixture.SparkSessionFixture
import com.capstone.quality.DataQuality
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Step 6: Batch pipeline integration tests. Tests the full batch pipeline end-to-end with sample
  * data, including data quality checks before and after transformations.
  */
class BatchPipelineSpec extends AnyFlatSpec with Matchers with SparkSessionFixture {

  val spark: org.apache.spark.sql.SparkSession = buildSparkSession()
  import spark.implicits._

  "BatchPipeline" should "process clean data successfully" in {
    val rawData = BatchPipeline.createSampleData(spark)

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    // Row count should be preserved (no nulls to drop)
    result.rowCount should be(10L)

    // Quality checks should all pass
    result.qualityReport.allPassed should be(true)

    // Output should have computed columns
    result.output.columns should contain("total_price")
    result.output.columns should contain("age_group")

    // Verify total_price is computed correctly
    val alice = result.output.filter(col("name") === "alice")
    val total = alice.select("total_price").as[Double].first()
    total shouldBe 59.98 +- 0.01 // 2 * 29.99
  }

  it should "drop rows with nulls in critical columns" in {
    val rawData = Seq(
      (1, "Alice", "Electronics", 2, 29.99, 30),
      (2, null, "Books", 1, 15.50, 25),
      (3, "Charlie", null, 3, 49.99, 70),
      (4, "Diana", "Clothing", 5, 12.00, 17)
    ).toDF("id", "name", "category", "quantity", "unit_price", "age")

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    // 2 rows dropped (null name, null category)
    result.rowCount should be(2L)
  }

  it should "remove duplicates based on unique key" in {
    val rawData = Seq(
      (1, "Alice", "Electronics", 2, 29.99, 30),
      (1, "Alice", "Books", 1, 15.50, 25),
      (2, "Bob", "Clothing", 3, 12.00, 17)
    ).toDF("id", "name", "category", "quantity", "unit_price", "age")

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    // 2 unique ids after dedup
    result.rowCount should be(2L)

    // Uniqueness check should pass
    val uniqueCheck = DataQuality.checkUniqueness(result.output, "id")
    uniqueCheck.passed should be(true)
  }

  it should "normalize string columns to lowercase" in {
    val rawData = Seq(
      (1, "  Alice  ", "  ELECTRONICS  ", 2, 29.99, 30),
      (2, " Bob ", " Books ", 1, 15.50, 25)
    ).toDF("id", "name", "category", "quantity", "unit_price", "age")

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    val names = result.output.select("name").as[String].collect().toSeq
    names should contain("alice")
    names should contain("bob")

    val categories = result.output.select("category").as[String].collect().toSeq
    categories should contain("electronics")
    categories should contain("books")
  }

  it should "categorize ages correctly" in {
    val rawData = Seq(
      (1, "Alice", "Electronics", 2, 29.99, 10),
      (2, "Bob", "Books", 1, 15.50, 25),
      (3, "Charlie", "Clothing", 3, 49.99, 70)
    ).toDF("id", "name", "category", "quantity", "unit_price", "age")

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    val groups = result.output.select("age_group").as[String].collect().toSeq
    groups should contain("child")
    groups should contain("adult")
    groups should contain("senior")
  }

  it should "work without age column" in {
    val rawData = Seq(
      (1, "Alice", "Electronics", 2, 29.99),
      (2, "Bob", "Books", 1, 15.50)
    ).toDF("id", "name", "category", "quantity", "unit_price")

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = None
    )

    result.rowCount should be(2L)
    result.output.columns should contain("total_price")
    result.output.columns should not contain ("age_group")
  }

  it should "check row counts before and after transformation" in {
    val rawData = Seq(
      (1, "Alice", "Electronics", 2, 29.99, 30),
      (2, null, "Books", 1, 15.50, 25),
      (3, "Charlie", "Clothing", 3, 49.99, 70),
      (4, "Diana", null, 5, 12.00, 17),
      (5, "Eve", "Books", 2, 22.00, 45)
    ).toDF("id", "name", "category", "quantity", "unit_price", "age")

    val rawCount = rawData.count()
    rawCount should be(5L)

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    // 2 rows dropped (null name, null category)
    result.rowCount should be(3L)
    result.rowCount should be < rawCount
  }

  it should "validate required columns are present in output" in {
    val rawData = BatchPipeline.createSampleData(spark)

    val result = BatchPipeline.run(
      spark = spark,
      rawData = rawData,
      requiredColumns = Seq("id", "name", "category", "quantity", "unit_price", "age"),
      uniqueKey = "id",
      noNullColumns = Seq("id", "name", "category"),
      stringColumns = Seq("name", "category"),
      quantityColumn = "quantity",
      priceColumn = "unit_price",
      ageColumn = Some("age")
    )

    val colCheck = DataQuality.checkColumnsExist(
      result.output,
      Seq("id", "name", "category", "quantity", "unit_price", "age", "total_price", "age_group")
    )
    colCheck.passed should be(true)
  }
}
