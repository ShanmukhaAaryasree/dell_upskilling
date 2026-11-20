ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "CustomerOrdersShuffleAssignment",
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.0.0",
      "org.apache.spark" %% "spark-sql"  % "4.0.0"
    )
  )