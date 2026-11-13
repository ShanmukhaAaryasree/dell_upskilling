ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.training"

val sparkVersion = "4.2.0"

lazy val root = rootProject
  .settings(
    name := "Employee_Spark_Scala3",
    Compile / mainClass := Some("com.training.EmployeeAnalysis"),

    libraryDependencies ++= Seq(
      // Keep Spark on the runtime classpath for local runs from IntelliJ and sbt.
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    Compile / run / fork := true,
    Compile / run / envVars += "SPARK_CONF_DIR" -> ((baseDirectory.value / "conf").getAbsolutePath),
    Compile / run / javaOptions ++= Seq(
      "--enable-native-access=ALL-UNNAMED",
      "-Dlog4j.configurationFile=log4j2.properties",
      "-Dlog4j2.configurationFile=log4j2.properties",
      "-Dspark.master=local[*]"
    ),
    Test / test := (Test / testFull).value,
    Test / fork := true,
    Test / envVars += "SPARK_CONF_DIR" -> ((baseDirectory.value / "conf").getAbsolutePath),
    Test / javaOptions ++= Seq(
      "--enable-native-access=ALL-UNNAMED",
      "-Dlog4j.configurationFile=log4j2.properties",
      "-Dlog4j2.configurationFile=log4j2.properties"
    ),
    Test / run / fork := true,
    Test / run / envVars += "SPARK_CONF_DIR" -> ((baseDirectory.value / "conf").getAbsolutePath),
    Test / run / javaOptions ++= Seq(
      "--enable-native-access=ALL-UNNAMED",
      "-Dlog4j.configurationFile=log4j2.properties",
      "-Dlog4j2.configurationFile=log4j2.properties",
      "-Dspark.master=local[*]"
    ),
    Test / parallelExecution := false,
    assembly / assemblyJarName := "employee-spark-assembly-0.1.0.jar"
  )
