# Customer Orders: RDD vs DataFrame Shuffle

A standalone Scala and Apache Spark application comparing RDD and DataFrame
implementations of common customer-order aggregations.

## What it demonstrates

- Customer-wise total order value
- Product-wise units sold
- Order counts by status
- RDD lineage and DataFrame physical plans for each aggregation

The sample orders are defined in the Scala source and processed locally with
Spark's `local[*]` master.

## Requirements

- Java 17 or later
- sbt 1.13.0

Spark Core and Spark SQL 4.0.0 are resolved by sbt.

## Run

From this directory:

```shell
sbt run
```

The application prints each result for both implementations, along with RDD
lineage and the DataFrame physical plan.
