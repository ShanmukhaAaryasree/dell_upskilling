object Assignment1CustomerValidation {

  case class Customer(
                       id: Option[Int],
                       name: Option[String],
                       age: Option[Int],
                       email: Option[String]
                     )

  // Validation helper functions returning Either[String, T]
  def validateId(idOpt: Option[Int]): Either[String, Int] = idOpt match {
    case Some(id) if id > 0 => Right(id)
    case Some(id)           => Left(s"Invalid ID ($id): ID must be positive")
    case None               => Left("Missing ID")
  }

  def validateName(nameOpt: Option[String]): Either[String, String] = nameOpt match {
    case Some(name) if name.trim.nonEmpty => Right(name.trim)
    case Some(_)                          => Left("Invalid Name: Name cannot be empty")
    case None                             => Left("Missing Name")
  }

  def validateAge(ageOpt: Option[Int]): Either[String, Int] = ageOpt match {
    case Some(age) if age >= 18 && age <= 100 => Right(age)
    case Some(age)                            => Left(s"Invalid Age ($age): Age must be between 18 and 100")
    case None                                 => Left("Missing Age")
  }

  def validateEmail(emailOpt: Option[String]): Either[String, String] = emailOpt match {
    case Some(email) if email.contains("@") && email.trim.length > 3 => Right(email.trim)
    case Some(email)                                                  => Left(s"Invalid Email ($email): Must contain '@'")
    case None                                                         => Left("Missing Email")
  }

  // Full record validation using for-comprehension on Either
  def validateCustomer(raw: Customer): Either[String, Customer] = {
    for {
      validId    <- validateId(raw.id)
      validName  <- validateName(raw.name)
      validAge   <- validateAge(raw.age)
      validEmail <- validateEmail(raw.email)
    } yield Customer(Some(validId), Some(validName), Some(validAge), Some(validEmail))
  }

  def main(args: Array[String]): Unit = {
    // Sample dataset with valid, missing, and invalid fields
    val incomingRecords: List[Customer] = List(
      Customer(Some(101), Some("Aarav Sharma"), Some(28), Some("aarav@example.com")),
      Customer(Some(-5), Some("Priya Verma"), Some(24), Some("priya@example.com")),
      Customer(Some(103), Some(""), Some(32), Some("rohan@example.com")),
      Customer(Some(104), Some("Sneha Gupta"), Some(16), Some("sneha@example.com")),
      Customer(Some(105), Some("Vikram Singh"), Some(45), Some("vikram.example.com")),
      Customer(None, Some("Ananya Roy"), Some(29), Some("ananya@example.com")),
      Customer(Some(107), Some("Kabir Das"), None, None),
      Customer(Some(108), Some("Meera Nair"), Some(35), Some("meera.nair@domain.org"))
    )

    // Process records and separate valid vs rejected using pattern matching & collection functions
    val results: List[(Customer, Either[String, Customer])] = incomingRecords.map { record =>
      (record, validateCustomer(record))
    }

    val validCustomers: List[Customer] = results.collect {
      case (_, Right(validCustomer)) => validCustomer
    }

    val rejectedRecords: List[(Customer, String)] = results.collect {
      case (rawRecord, Left(errorReason)) => (rawRecord, errorReason)
    }

    // Expected Output
    println("=== Customer Data Validation Pipeline Report ===")
    println(s"Total records received : ${incomingRecords.size}")
    println(s"Valid customer count   : ${validCustomers.size}")
    println(s"Rejected customer count: ${rejectedRecords.size}")

    println("\n--- Valid Customer Details ---")
    validCustomers.foreach {
      case Customer(Some(id), Some(name), Some(age), Some(email)) =>
        println(f"ID: $id%-4d | Name: $name%-15s | Age: $age%-3d | Email: $email")
      case _ => ()
    }

    println("\n--- Rejected Records with Error Reasons ---")
    rejectedRecords.zipWithIndex.foreach { case ((rawRecord, reason), index) =>
      println(s"${index + 1}. Record: $rawRecord -> Reason: $reason")
    }
  }
}