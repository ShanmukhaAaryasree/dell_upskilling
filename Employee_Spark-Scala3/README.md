# Employee Spark Analysis

A Scala application that runs employee data through Apache Spark SQL. The project includes reusable DataFrame transformations for salary updates, filtering, sorting, aggregation, and data-quality checks.

## Requirements

- Java 17 or newer
- sbt 2.0.12 (configured in `project/build.properties`)

Spark SQL 4.2.0 and the other dependencies are resolved by sbt.

## Run

From this directory, run:

```powershell
.\run-employee-spark.ps1 -LocalSbt
```

Or invoke the application directly:

```shell
sbt "Compile / runMain com.training.EmployeeAnalysis"
```

The sample output is saved in [`outputs/employee-analysis-output.txt`](outputs/employee-analysis-output.txt).

## Project structure

```text
.
├── build.sbt
├── conf/
├── outputs/
├── project/
└── src/
    ├── main/scala/com/training/
    └── test/scala/com/training/
```

Generated build files and IDE-specific settings are intentionally excluded.

## Tests

The existing test sources currently do not compile against the configured Spark and ScalaTest APIs. The application itself can be compiled and run with the command above.
