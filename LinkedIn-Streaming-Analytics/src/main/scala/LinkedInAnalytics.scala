
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LinkedInAnalytics {

  def main(args: Array[String]): Unit = {
    val bootstrapServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    val spark = SparkSession.builder()
      .appName("LinkedIn Real-Time Interaction Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define the incoming JSON event schema
    val eventSchema = new StructType()
      .add("eventId", StringType)
      .add("memberId", StringType)
      .add("postId", StringType)
      .add("eventType", StringType)
      .add("eventTime", StringType)

    // 1. Read real-time events from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "linkedin-events")
      .option("startingOffsets", "earliest")
      .load()

    // 2. Parse JSON messages into a structured DataFrame
    val events = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), eventSchema).as("data"))
      .select("data.*")
      .withColumn(
        "eventTime",
        to_timestamp(col("eventTime"), "yyyy-MM-dd'T'HH:mm:ssX")
      )
      .filter(col("eventTime").isNotNull)

    // 3. Calculate total views, likes and comments per post
    val postAnalytics = events
      .groupBy("postId")
      .agg(
        sum(when(col("eventType") === "view", 1).otherwise(0)).as("totalViews"),
        sum(when(col("eventType") === "like", 1).otherwise(0)).as("totalLikes"),
        sum(when(col("eventType") === "comment", 1).otherwise(0)).as("totalComments")
      )

    val postQuery = postAnalytics.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        new java.io.File("checkpoints/post-analytics").getAbsolutePath
      )
      .start()

    // 4. Calculate unique members interacting with each post
    val uniqueMembers = events
      .withWatermark("eventTime", "10 minutes")
      .dropDuplicates("postId", "memberId")
      .groupBy("postId")
      .count()

    val uniqueQuery = uniqueMembers.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        new java.io.File("checkpoints/unique-members").getAbsolutePath
      )
      .start()

    // 5. Calculate one-minute event-time window analytics
    val windowAnalytics = events
      .withWatermark("eventTime", "10 minutes")
      .groupBy(
        window(col("eventTime"), "1 minute"),
        col("postId")
      )
      .agg(
        sum(when(col("eventType") === "view", 1).otherwise(0)).as("views"),
        sum(when(col("eventType") === "like", 1).otherwise(0)).as("likes"),
        sum(when(col("eventType") === "comment", 1).otherwise(0)).as("comments")
      )

    val windowQuery = windowAnalytics.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        new java.io.File("checkpoints/window-analytics").getAbsolutePath
      )
      .start()

    // 6. Keep all streaming queries active
    spark.streams.awaitAnyTermination()
  }
}
