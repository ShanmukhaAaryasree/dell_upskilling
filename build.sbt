ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-typed-datasets",
    Compile / run / fork := true,
    Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3"
    )
  )