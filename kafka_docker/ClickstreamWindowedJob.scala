package com.training


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object ClickstreamWindowedJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ClickstreamKafkaToParquet")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventSchema = new StructType()
      .add("user", StringType)
      .add("page", StringType)
      .add("ts", StringType)

    // 1. Read events from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "clickstream")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", "1000")
      .load()

    // 2. Parse JSON messages
    val events = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json")
      .filter(col("json").isNotNull && length(trim(col("json"))) > 0)
      .select(from_json(col("json"), eventSchema).as("data"))
      .select(
        col("data.user").as("user"),
        col("data.page").as("page"),
        to_timestamp(
          col("data.ts"),
          "yyyy-MM-dd'T'HH:mm:ssX"
        ).as("event_time")
      )
      .filter(
        col("user").isNotNull &&
          col("page").isNotNull &&
          col("event_time").isNotNull
      )
      .withWatermark("event_time", "1 minute")

    // 3. Calculate clickstream window aggregates
    val windowed = events
      .groupBy(
        window(col("event_time"), "30 seconds"),
        col("page")
      )
      .agg(
        count("*").as("clicks"),
        collect_set("user").as("users")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("page"),
        col("clicks"),
        size(col("users")).as("unique_users")
      )

    // 4. Write results to Parquet
    val query = windowed.writeStream
      .format("parquet")
      .outputMode("append")
      .option(
        "path",
        "C:/Users/Itinfra/IdeaProjects/ClickstreamWindowedJob/output/parquet/clickstream_windows"
      )
      .option(
        "path",
        "C:/Users/Itinfra/IdeaProjects/ClickstreamWindowedJob/output/parquet/clickstream_windows"
      )
      .option("checkpointLocation", "C:/spark-checkpoints/clickstream-windowed-job")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // 5. Keep the streaming application running
    query.awaitTermination()
  }
}
