# Spark Typed Datasets

A small Scala application that demonstrates the difference between Spark DataFrames and strongly typed Datasets using employee data.

## What it demonstrates

- Filtering and sorting employees with a DataFrame
- Filtering and sorting typed `Dataset[Employee]` records
- Projecting records into a typed view
- Mapping employees into annual and monthly salary summaries

The sample data is defined in `TypedDatasetDemo` and runs locally with Spark's `local[*]` master.

## Requirements

- Java 17
- sbt 1.13.1 (the sbt launcher uses the version configured in `project/build.properties`)

Dependencies, including Apache Spark SQL 3.5.3, are resolved by sbt.

## Run

From the project directory:

```shell
sbt run
```

The application prints the results for each DataFrame and Dataset operation to the console.

## Project structure

```text
.
├── build.sbt
├── project/
└── src/main/scala/com/example/TypedDatasetDemo.scala
```

## Employee Spark Analysis

The [`Employee_Spark-Scala3`](Employee_Spark-Scala3/) folder contains a separate employee-analysis project with reusable Spark DataFrame transformations and a captured sample output.

See its [README](Employee_Spark-Scala3/README.md) for requirements and run instructions. The captured output is in [`Employee_Spark-Scala3/outputs/employee-analysis-output.txt`](Employee_Spark-Scala3/outputs/employee-analysis-output.txt).
