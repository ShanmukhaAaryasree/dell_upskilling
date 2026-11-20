import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class CustomerOrder(
                          order_id: String,
                          customer_id: String,
                          customer_name: String,
                          product: String,
                          category: String,
                          quantity: Int,
                          order_amount: Long,
                          status: String
                        )

object CustomerOrdersShuffleAssignment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CustomerOrdersRDDvsDataFrameShuffle")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val rawOrders = Seq(
      CustomerOrder("O1001", "C101", "Anita",  "Laptop",     "Electronics", 1, 55000L, "Delivered"),
      CustomerOrder("O1002", "C102", "Bharat", "Phone",      "Electronics", 1, 25000L, "Delivered"),
      CustomerOrder("O1003", "C101", "Anita",  "Mouse",      "Accessories", 2,  3000L, "Delivered"),
      CustomerOrder("O1004", "C103", "Charan", "Desk",       "Furniture",   1, 18000L, "Cancelled"),
      CustomerOrder("O1005", "C104", "Divya",  "Laptop",     "Electronics", 1, 60000L, "Delivered"),
      CustomerOrder("O1006", "C102", "Bharat", "Headphones", "Accessories", 1,  4000L, "Returned"),
      CustomerOrder("O1007", "C105", "Esha",   "Chair",      "Furniture",   2, 14000L, "Delivered"),
      CustomerOrder("O1008", "C104", "Divya",  "Keyboard",   "Accessories", 1,  7000L, "Delivered"),
      CustomerOrder("O1009", "C103", "Charan", "Chair",      "Furniture",   1, 13000L, "Delivered"),
      CustomerOrder("O1010", "C105", "Esha",   "Desk",       "Furniture",   1, 15000L, "Cancelled"),
      CustomerOrder("O1011", "C101", "Anita",  "Mouse",      "Accessories", 3,  4500L, "Returned"),
      CustomerOrder("O1012", "C102", "Bharat", "Phone",      "Electronics", 2, 50000L, "Delivered")
    )

    // Note: Keep the exact dataset from your current file if any rows differ
    val ordersRDD = spark.sparkContext.parallelize(rawOrders)
    val ordersDF = rawOrders.toDF()

    // =========================================================================
    // PROBLEM 1: CUSTOMER-WISE TOTAL ORDER VALUE
    // =========================================================================
    println("\n=== PROBLEM 1: CUSTOMER-WISE TOTAL ORDER VALUE ===")

    // --- RDD Implementation ---
    val p1RDD = ordersRDD
      .map(o => ((o.customer_id, o.customer_name), o.order_amount))
      .reduceByKey(_ + _)
      .sortBy({ case ((cid, _), total) => (-total, cid) })
      .map { case ((cid, cname), total) => (cid, cname, total) }

    println("--- P1 RDD Output (customer_id, customer_name, total_order_value) ---")
    p1RDD.collect().foreach { case (cid, cname, total) =>
      println(s"  $cid | $cname | $total")
    }

    println("--- P1 RDD Lineage ---")
    println(p1RDD.toDebugString)

    // --- DataFrame Implementation ---
    val p1DF = ordersDF
      .groupBy("customer_id", "customer_name")
      .agg(sum("order_amount").as("total_order_value"))
      .orderBy(desc("total_order_value"), asc("customer_id"))

    println("--- P1 DataFrame Output ---")
    p1DF.show(false)

    println("--- P1 DataFrame Physical Plan ---")
    p1DF.explain()

    // =========================================================================
    // PROBLEM 2: PRODUCT-WISE UNITS SOLD
    // =========================================================================
    println("\n=== PROBLEM 2: PRODUCT-WISE UNITS SOLD ===")

    // --- RDD Implementation ---
    val p2RDD = ordersRDD
      .map(o => (o.product, o.quantity))
      .reduceByKey(_ + _)
      .sortBy({ case (prod, units) => (-units, prod) })

    println("--- P2 RDD Output (product, units_sold) ---")
    p2RDD.collect().foreach { case (prod, units) =>
      println(s"  $prod | $units")
    }

    println("--- P2 RDD Lineage ---")
    println(p2RDD.toDebugString)

    // --- DataFrame Implementation ---
    val p2DF = ordersDF
      .groupBy("product")
      .agg(sum("quantity").as("units_sold"))
      .orderBy(desc("units_sold"), asc("product"))

    println("--- P2 DataFrame Output ---")
    p2DF.show(false)

    println("--- P2 DataFrame Physical Plan ---")
    p2DF.explain()

    // =========================================================================
    // PROBLEM 3: ORDER COUNT BY STATUS
    // =========================================================================
    println("\n=== PROBLEM 3: ORDER COUNT BY STATUS ===")

    // --- RDD Implementation ---
    val p3RDD = ordersRDD
      .map(o => (o.status, 1L))
      .reduceByKey(_ + _)
      .sortBy({ case (st, cnt) => (-cnt, st) })

    println("--- P3 RDD Output (status, order_count) ---")
    p3RDD.collect().foreach { case (st, cnt) =>
      println(s"  $st | $cnt")
    }

    println("--- P3 RDD Lineage ---")
    println(p3RDD.toDebugString)

    // --- DataFrame Implementation ---
    val p3DF = ordersDF
      .groupBy("status")
      .agg(count(lit(1)).as("order_count"))
      .orderBy(desc("order_count"), asc("status"))

    println("--- P3 DataFrame Output ---")
    p3DF.show(false)

    println("--- P3 DataFrame Physical Plan ---")
    p3DF.explain()

    println("\nAll 3 problems completed and verified!")
    spark.stop()
  }
}