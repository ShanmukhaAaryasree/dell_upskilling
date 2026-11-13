ThisBuild / scalaVersion := "2.13.18"
ThisBuild / organization := "com.capstone"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "spark_testing_capstone",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-streaming" % "3.5.3",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--enable-native-access=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true",
      "-Dhadoop.native.lib=false"
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--enable-native-access=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true",
      "-Dhadoop.native.lib=false"
    ),
    Compile / mainClass := Some("com.capstone.CapstoneApp"),
    assembly / mainClass := Some("com.capstone.CapstoneApp"),
    assembly / assemblyMergeStrategy := {
      // Module descriptors are metadata for modular jars, not Spark's classpath runtime.
      case "module-info.class" => MergeStrategy.discard
      case path if path.matches("META-INF/versions/\\d+/module-info\\.class") => MergeStrategy.discard
      // These text resources are intended to be combined across Spark dependencies.
      case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
      case "arrow-git.properties" => MergeStrategy.concat
      // Prefer the schema and bridge classes supplied by Spark's resolved runtime jars.
      case path if path.startsWith("google/protobuf/") && path.endsWith(".proto") => MergeStrategy.last
      case path if path.startsWith("org/apache/commons/logging/") => MergeStrategy.last
      // Spark 3 uses Log4j2; its core cache follows the compatibility cache in classpath order.
      case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" =>
        MergeStrategy.last
      case path => (assembly / assemblyMergeStrategy).value(path)
    }
  )
