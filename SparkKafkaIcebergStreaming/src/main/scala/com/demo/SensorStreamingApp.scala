
package com.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object SensorStreamingApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("sensor-kafka-to-iceberg")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.local.type", "hadoop")
      .config(
        "spark.sql.catalog.local.warehouse",
        "file:///data/warehouse"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {

      // Create Iceberg namespace
      spark.sql("CREATE NAMESPACE IF NOT EXISTS local.iot")

      // Create Iceberg table
      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.iot.sensor_readings (
          device_id STRING,
          temperature DOUBLE,
          event_time TIMESTAMP
        )
        USING iceberg
      """)

      // Define incoming JSON schema
      val jsonSchema = new StructType()
        .add("deviceId", StringType)
        .add("temperature", DoubleType)
        .add("eventTime", StringType)

      // Read streaming data from Kafka
      val kafkaStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "sensor-data")
        .option("startingOffsets", "latest")
        .load()

      // Parse and transform sensor records
      val sensorRecords = kafkaStream
        .select(
          from_json(
            col("value").cast("string"),
            jsonSchema
          ).as("record")
        )
        .filter(col("record").isNotNull)
        .select(
          col("record.deviceId").as("device_id"),
          col("record.temperature").as("temperature"),
          to_timestamp(col("record.eventTime")).as("event_time")
        )
        .filter(
          col("device_id").isNotNull &&
            col("event_time").isNotNull
        )

      // Write streaming data into Iceberg
      sensorRecords.writeStream
        .format("iceberg")
        .outputMode("append")
        .option(
          "checkpointLocation",
          "/data/checkpoints/sensor-readings-v2"
        )
        .trigger(Trigger.ProcessingTime("1 minute"))
        .toTable("local.iot.sensor_readings")
        .awaitTermination()

    } finally {
      spark.stop()
    }
  }
}
