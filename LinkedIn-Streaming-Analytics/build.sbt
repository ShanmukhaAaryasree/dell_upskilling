
name := "LinkedIn-Streaming-Analytics"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.9",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9"
)
