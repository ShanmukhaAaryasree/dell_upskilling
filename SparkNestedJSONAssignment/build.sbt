ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SparkNestedJSONAssignment",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
  )

Compile / run / fork := true

// Spark 3.5.3 requires a Java 17 runtime. Set JAVA17_HOME if JDK 17 is installed elsewhere.
Compile / run / javaHome := Some(file(sys.env.getOrElse(
  "JAVA17_HOME",
  "C:/Program Files/Eclipse Adoptium/jdk-17.0.21.9-hotspot"
)))

Compile / run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
