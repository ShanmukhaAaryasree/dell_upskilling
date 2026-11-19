ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"
ThisBuild / organization := "com.demo"

lazy val root = (project in file("."))
  .settings(
    name := "spark-kafka-iceberg",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.9" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9",
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.12.0"
    ),

    assembly / assemblyJarName := "spark-kafka-iceberg.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
