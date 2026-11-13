package com.capstone.customer

/** Customer data accepted by the application. */
final case class Customer(id: String, name: String, email: String)

/** Basic validation for customer records before they enter a pipeline. */
object CustomerValidation {
  private val EmailPattern = "^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$".r

  /** Returns all validation errors, or the customer when its required fields are valid. */
  def validate(customer: Customer): Either[Seq[String], Customer] = {
    val errors = Seq(
      Option.when(Option(customer.id).forall(_.trim.isEmpty))("Customer ID must not be blank"),
      Option.when(Option(customer.name).forall(_.trim.isEmpty))("Customer name must not be blank"),
      Option.when(
        Option(customer.email).forall(email => !EmailPattern.matches(email.trim))
      )("Customer email must be a valid email address")
    ).flatten

    if (errors.isEmpty) Right(customer) else Left(errors)
  }
}
