package com.capstone.customer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CustomerValidationSpec extends AnyFlatSpec with Matchers {
  "CustomerValidation" should "accept a customer with an ID, name, and valid email" in {
    val customer = Customer("C-1001", "Ada Lovelace", "ada@example.com")

    CustomerValidation.validate(customer) should be(Right(customer))
  }

  it should "reject a blank customer ID" in {
    val result = CustomerValidation.validate(Customer("  ", "Ada Lovelace", "ada@example.com"))

    result should be(Left(Seq("Customer ID must not be blank")))
  }

  it should "report each invalid required field" in {
    val result = CustomerValidation.validate(Customer(null, " ", "not-an-email"))

    result should be(
      Left(
        Seq(
          "Customer ID must not be blank",
          "Customer name must not be blank",
          "Customer email must be a valid email address"
        )
      )
    )
  }
}
