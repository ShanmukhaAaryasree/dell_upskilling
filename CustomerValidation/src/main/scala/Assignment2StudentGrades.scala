object Assignment2StudentGrades {

  case class Subject(
                      name: String,
                      marks: Option[Int]
                    )

  case class Student(
                      id: Int,
                      name: String,
                      subjects: List[Subject]
                    )

  val PassThresholdPercentage = 40.0
  val DistinctionThresholdPercentage = 75.0
  val MaxMarksPerSubject = 100

  // Validation function returning Either[String, Int] (Left = incomplete/invalid reason, Right = total marks)
  def validateAndCalculateTotal(student: Student): Either[String, Int] = student match {
    case Student(_, name, Nil) =>
      Left(s"$name has no subjects enrolled")

    case Student(_, name, subjects) =>
      // Check if any subject has missing or out-of-range marks using pattern matching
      val missingSubjects = subjects.collect {
        case Subject(subName, None) => s"$subName (missing marks)"
        case Subject(subName, Some(m)) if m < 0 || m > MaxMarksPerSubject =>
          s"$subName (invalid marks: $m)"
      }

      if (missingSubjects.nonEmpty) {
        Left(s"Incomplete assessment for $name: ${missingSubjects.mkString(", ")}")
      } else {
        // Safely extract marks using flatMap on Option
        val total = subjects.flatMap(_.marks).sum
        Right(total)
      }
  }

  def calculatePercentage(totalMarks: Int, subjectCount: Int): Double = {
    if (subjectCount == 0) 0.0
    else (totalMarks.toDouble / (subjectCount * MaxMarksPerSubject)) * 100.0
  }

  def main(args: Array[String]): Unit = {
    val students: List[Student] = List(
      Student(1, "Aditi Rao", List(
        Subject("Mathematics", Some(88)),
        Subject("Physics", Some(82)),
        Subject("Computer Science", Some(91))
      )),
      Student(2, "Rohan Mehta", List(
        Subject("Mathematics", Some(62)),
        Subject("Physics", Some(58)),
        Subject("Computer Science", Some(65))
      )),
      Student(3, "Neha Kapoor", List(
        Subject("Mathematics", Some(78)),
        Subject("Physics", None),
        Subject("Computer Science", Some(84))
      )),
      Student(4, "Siddharth Joshi", List(
        Subject("Mathematics", Some(32)),
        Subject("Physics", Some(38)),
        Subject("Computer Science", Some(41))
      )),
      Student(5, "Pooja Nair", List(
        Subject("Mathematics", Some(95)),
        Subject("Physics", Some(90)),
        Subject("Computer Science", Some(94))
      )),
      Student(6, "Karan Malhotra", List(
        Subject("Mathematics", None),
        Subject("Physics", None),
        Subject("Computer Science", Some(70))
      ))
    )

    // Evaluate each student
    val evaluatedStudents: List[(Student, Either[String, Int])] = students.map { s =>
      (s, validateAndCalculateTotal(s))
    }

    // 1. Student-wise total marks (summing available marks using flatMap)
    println("=== 1. Student-Wise Total Marks ===")
    students.foreach {
      case Student(id, name, subjects) =>
        val availableTotal = subjects.flatMap(_.marks).sum
        val completedCount = subjects.count(_.marks.isDefined)
        println(f"ID: $id%-2d | Name: $name%-16s | Available Total: $availableTotal%-3d (${completedCount}/${subjects.size} subjects graded)")
    }

    // Separate complete vs incomplete assessments using pattern matching
    val completeStudents: List[(Student, Int, Double)] = evaluatedStudents.collect {
      case (student, Right(total)) =>
        val pct = calculatePercentage(total, student.subjects.size)
        (student, total, pct)
    }

    val incompleteStudents: List[(Student, String)] = evaluatedStudents.collect {
      case (student, Left(reason)) => (student, reason)
    }

    // 2. Students who passed (complete assessment & >= 40% overall and in all subjects)
    val passedStudents = completeStudents.filter { case (student, _, pct) =>
      val passedAllSubjects = student.subjects.forall {
        case Subject(_, Some(m)) => m >= 40
        case _                   => false
      }
      pct >= PassThresholdPercentage && passedAllSubjects
    }

    println("\n=== 2. Students Who Passed ===")
    passedStudents.foreach { case (s, total, pct) =>
      println(f"ID: ${s.id}%-2d | Name: ${s.name}%-16s | Total: $total%-3d | Percentage: $pct%.2f%%")
    }

    // 3. Students with incomplete assessments
    println("\n=== 3. Students With Incomplete Assessments ===")
    incompleteStudents.foreach { case (s, reason) =>
      println(s"ID: ${s.id} | Name: ${s.name} -> $reason")
    }

    // 4. Students with distinction (75% or above)
    val distinctionStudents = completeStudents.filter { case (_, _, pct) =>
      pct >= DistinctionThresholdPercentage
    }

    println("\n=== 4. Students With Distinction (75% or Above) ===")
    distinctionStudents.foreach { case (s, total, pct) =>
      println(f"ID: ${s.id}%-2d | Name: ${s.name}%-16s | Total: $total%-3d | Percentage: $pct%.2f%%")
    }
  }
}