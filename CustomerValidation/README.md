# Customer Validation and Student Grades

This standalone Scala project contains two small assignments:

- **Customer validation** checks that each record has a positive ID, a non-empty
  name, an age from 18 through 100, and an email containing `@`. It reports
  valid customers and the first validation error for rejected records.
- **Student grades** reports available marks, identifies incomplete
  assessments, checks pass requirements (at least 40% overall and 40 marks in
  every subject), and lists students with distinction (at least 75% overall).

## Requirements

- Java
- sbt (version is specified in `project/build.properties`)

## Run

From this directory, run either assignment:

```shell
sbt "runMain Assignment1CustomerValidation"
sbt "runMain Assignment2StudentGrades"
```

## Structure

```text
CustomerValidation/
├── build.sbt
├── project/
│   └── build.properties
└── src/main/scala/
    ├── Assignment1CustomerValidation.scala
    └── Assignment2StudentGrades.scala
```
