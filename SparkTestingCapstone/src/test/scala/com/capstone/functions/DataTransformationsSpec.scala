package com.capstone.functions

import com.capstone.fixture.SparkSessionFixture
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Step 3: Unit tests for pure transformation functions. Each test creates small, deterministic
  * DataFrames and verifies the output.
  */
class DataTransformationsSpec extends AnyFlatSpec with Matchers with SparkSessionFixture {

  val spark: org.apache.spark.sql.SparkSession = buildSparkSession()
  import spark.implicits._

  // --- dropNulls ---

  "dropNulls" should "remove rows with nulls in specified columns" in {
    val data = Seq(
      (1, Some("Alice"), Some(30)),
      (2, None, Some(25)),
      (3, Some("Charlie"), None),
      (4, Some("Diana"), Some(40))
    ).toDF("id", "name", "age")

    val result = DataTransformations.dropNulls(data, Seq("name", "age"))
    result.count() should be(2)
    result.collect().map(_.getInt(0)).toSet should be(Set(1, 4))
  }

  it should "return all rows when no nulls exist" in {
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 25)
    ).toDF("id", "name", "age")

    val result = DataTransformations.dropNulls(data, Seq("name", "age"))
    result.count() should be(2)
  }

  it should "return original DataFrame when column list is empty" in {
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 25)
    ).toDF("id", "name", "age")

    val result = DataTransformations.dropNulls(data, Seq.empty[String])
    result.count() should be(2)
  }

  // --- dropDuplicates ---

  "dropDuplicates" should "remove duplicate rows based on key columns" in {
    val data = Seq(
      (1, "Alice", "A"),
      (1, "Alice", "B"),
      (2, "Bob", "A"),
      (3, "Charlie", "A")
    ).toDF("id", "name", "category")

    val result = DataTransformations.dropDuplicates(data, Seq("id"))
    result.count() should be(3)
  }

  it should "use distinct when key columns are empty" in {
    val data = Seq(
      (1, "Alice"),
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    val result = DataTransformations.dropDuplicates(data, Seq.empty[String])
    result.count() should be(2)
  }

  // --- renameColumns ---

  "renameColumns" should "rename columns according to mapping" in {
    val data = Seq(
      (1, "Alice", 30)
    ).toDF("id", "name", "age")

    val result =
      DataTransformations.renameColumns(data, Map("name" -> "full_name", "age" -> "years"))
    result.columns.toSet should be(Set("id", "full_name", "years"))
  }

  it should "leave DataFrame unchanged with empty mapping" in {
    val data = Seq(
      (1, "Alice", 30)
    ).toDF("id", "name", "age")

    val result = DataTransformations.renameColumns(data, Map.empty[String, String])
    result.columns.toSet should be(Set("id", "name", "age"))
  }

  // --- selectColumns ---

  "selectColumns" should "select only specified columns in order" in {
    val data = Seq(
      (1, "Alice", 30)
    ).toDF("id", "name", "age")

    val result = DataTransformations.selectColumns(data, Seq("name", "id"))
    result.columns should be(Seq("name", "id"))
    result.count() should be(1)
  }

  // --- addComputedColumn ---

  "addComputedColumn" should "add a new computed column" in {
    val data = Seq(
      (1, 10, 20),
      (2, 30, 40)
    ).toDF("id", "x", "y")

    val result = DataTransformations.addComputedColumn(data, "sum_xy", col("x") + col("y"))
    result.columns should contain("sum_xy")
    val rows = result.collect()
    rows(0).getInt(3) should be(30)
    rows(1).getInt(3) should be(70)
  }

  // --- filterByCondition ---

  "filterByCondition" should "filter rows by a condition" in {
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 15),
      (3, "Charlie", 70)
    ).toDF("id", "name", "age")

    val result = DataTransformations.filterByCondition(data, col("age") > 18)
    result.count() should be(2)
  }

  // --- normalizeStrings ---

  "normalizeStrings" should "convert strings to lowercase and trim whitespace" in {
    val data = Seq(
      (1, "  Alice  ", "  ELECTRONICS  "),
      (2, " Bob ", " Books ")
    ).toDF("id", "name", "category")

    val result = DataTransformations.normalizeStrings(data, Seq("name", "category"))
    val names = result.select("name").as[String].collect().toSeq
    names should contain("alice")
    names should contain("bob")

    val categories = result.select("category").as[String].collect().toSeq
    categories should contain("electronics")
    categories should contain("books")
  }

  // --- fillNulls ---

  "fillNulls" should "replace nulls with default values" in {
    val data = Seq(
      (1, Some("Alice"), Some(30)),
      (2, None, Some(25)),
      (3, Some("Charlie"), None)
    ).toDF("id", "name", "age")

    val result = DataTransformations.fillNulls(data, Map("name" -> "Unknown", "age" -> 0))
    val nullNames = result.filter(col("name") === "Unknown").count()
    nullNames should be(1L)
    val nullAges = result.filter(col("age") === 0).count()
    nullAges should be(1L)
  }

  // --- categorizeAge ---

  "categorizeAge" should "categorize ages correctly" in {
    val data = Seq(
      (1, "Alice", 10),
      (2, "Bob", 25),
      (3, "Charlie", 70)
    ).toDF("id", "name", "age")

    val result = DataTransformations.categorizeAge(data, "age", "age_group")
    val groups = result.select("age_group").as[String].collect().toSeq
    groups should contain("child")
    groups should contain("adult")
    groups should contain("senior")
  }

  // --- computeTotalPrice ---

  "computeTotalPrice" should "compute quantity * unit_price" in {
    val data = Seq(
      (1, 2, 29.99),
      (2, 3, 15.50)
    ).toDF("id", "quantity", "unit_price")

    val result =
      DataTransformations.computeTotalPrice(data, "quantity", "unit_price", "total_price")
    val totals = result.select("total_price").as[Double].collect().toSeq
    totals(0) shouldBe 59.98 +- 0.01
    totals(1) shouldBe 46.50 +- 0.01
  }

  // --- aggregateSum ---

  "aggregateSum" should "aggregate by group columns" in {
    val data = Seq(
      (1, "A", 10),
      (2, "A", 20),
      (3, "B", 30)
    ).toDF("id", "category", "value")

    val result = DataTransformations.aggregateSum(data, Seq("category"), "value", "total_value")
    result.count() should be(2)
    val aTotal = result.filter(col("category") === "A").select("total_value").as[Double].first()
    aTotal shouldBe 30.0 +- 0.01
  }

  // --- sortByColumns ---

  "sortByColumns" should "sort by specified columns" in {
    val data = Seq(
      (3, "Charlie"),
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    val result = DataTransformations.sortByColumns(data, Seq("id"))
    val ids = result.select("id").as[Int].collect().toSeq
    ids should be(Seq(1, 2, 3))
  }

  // --- applyPipeline ---

  "applyPipeline" should "apply a sequence of transformations" in {
    val data = Seq(
      (1, "  Alice  ", 2, 10.0, 30),
      (2, " Bob ", 3, 20.0, 15)
    ).toDF("id", "name", "quantity", "unit_price", "age")

    val pipeline = Seq[(org.apache.spark.sql.DataFrame) => org.apache.spark.sql.DataFrame](
      (df: org.apache.spark.sql.DataFrame) => DataTransformations.normalizeStrings(df, Seq("name")),
      (df: org.apache.spark.sql.DataFrame) =>
        DataTransformations.computeTotalPrice(df, "quantity", "unit_price", "total_price"),
      (df: org.apache.spark.sql.DataFrame) =>
        DataTransformations.categorizeAge(df, "age", "age_group")
    )

    val result = DataTransformations.applyPipeline(data, pipeline)
    result.columns should contain("name")
    result.columns should contain("total_price")
    result.columns should contain("age_group")
    result.count() should be(2)

    val names = result.select("name").as[String].collect().toSeq
    names should contain("alice")
    names should contain("bob")
  }

  it should "return original DataFrame with empty pipeline" in {
    val data = Seq(
      (1, "Alice", 30)
    ).toDF("id", "name", "age")

    val result = DataTransformations.applyPipeline(data, Seq.empty)
    result.count() should be(1)
    result.columns.toSet should be(Set("id", "name", "age"))
  }
}
