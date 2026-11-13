# Spark Testing Capstone

This folder contains a Scala/Spark project for batch data processing, streaming pipeline examples, customer validation, and data-quality checks. The batch application runs against deterministic sample sales data.

## Requirements

- JDK 21 (also tested locally with JDK 17)
- sbt 1.13.1
- IntelliJ IDEA with the Scala plugin (optional)

## Run

From this directory:

```shell
sbt run
sbt test
```

The sample application uses `SPARK_MASTER` when set and otherwise runs with `local[*]`. Its captured console output is in [`outputs/sample-run-output.txt`](outputs/sample-run-output.txt).

## Project layout

```text
src/main/scala/com/capstone/  Application, pipelines, transformations, and quality checks
src/test/scala/com/capstone/  ScalaTest suites
project/                      Pinned sbt version and build plugins
outputs/                      Captured sample application output
```

For formatting, static checks, and packaging, run `sbt scalafmtCheckAll`, `sbt scalafixAll`, and `sbt assembly`.
