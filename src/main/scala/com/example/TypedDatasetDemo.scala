package com.example

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
                     id: Long,
                     name: String,
                     department: String,
                     salary: Double
                   )

case class EmployeePayView(
                            name: String,
                            department: String,
                            salary: Double
                          )

case class EmployeeSummary(
                            name: String,
                            annualSalary: Double,
                            monthlySalary: Double
                          )

object TypedDatasetDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TypedDatasetDemo")
      .master("local[*]")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("ERROR")
      import spark.implicits._

      val employees = Seq(
        Employee(1L, "Asha", "Engineering", 90000.0),
        Employee(2L, "Ravi", "Sales", 65000.0),
        Employee(3L, "Meera", "Engineering", 98000.0),
        Employee(4L, "John", "HR", 60000.0)
      )

      val employeeDF: DataFrame = employees.toDF()

      val highEarnersDF = employeeDF
        .filter(col("salary") >= 70000.0)
        .select(col("name"), col("department"), col("salary"))
        .orderBy(col("salary").desc)

      println("=== Untyped DataFrame result ===")
      highEarnersDF.show(truncate = false)

      val employeeDS: Dataset[Employee] = employees.toDS()

      val highEarnersDS = employeeDS
        .filter(employee => employee.salary >= 70000.0)
        .orderBy($"salary".desc)

      println("=== Typed Dataset result ===")
      highEarnersDS.show(truncate = false)

      val payViewDS: Dataset[EmployeePayView] =
        employeeDS
          .filter(_.salary >= 70000.0)
          .map(e => EmployeePayView(e.name, e.department, e.salary))

      println("=== Typed projection result ===")
      payViewDS.show(truncate = false)

      val summaries: Dataset[EmployeeSummary] =
        employeeDS.map { employee =>
          EmployeeSummary(
            name = employee.name,
            annualSalary = employee.salary,
            monthlySalary = employee.salary / 12.0
          )
        }

      println("=== Typed map result ===")
      summaries.show(truncate = false)
    } finally {
      spark.stop()
    }
  }
}