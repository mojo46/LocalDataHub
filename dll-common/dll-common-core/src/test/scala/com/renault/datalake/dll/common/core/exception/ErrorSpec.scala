package com.renault.datalake.dll.common.core.exception

import java.io.IOException

import com.renault.datalake.dll.common.core.exception.DataLakeErrors._
import com.renault.datalake.dll.common.core.exception.ErrorSpec.SpecError
import com.renault.datalake.dll.common.core.exception.specific.Transformer.{InvalidCast, TransformError}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

import scala.util.Try

class ErrorSpec extends FeatureSpec with Matchers with GivenWhenThen {

  feature("DLL can accumulate errors in a sequence") {

    scenario("a DataLakeErrors can be instantiate from a any error") {
      val errors = Try(Option.empty[Nothing].get).failed.get.toErrors

      errors shouldBe a[DataLakeErrors]
      errors.head shouldBe a[NoSuchElementException]
      errors.head.getMessage shouldBe "None.get"
      errors.tail shouldBe empty
    }

    scenario("a error can be add on top of a DataLakeErrors") {
      val errors = DataLakeErrors(new IOException("I/O#1"), Vector(new IOException("I/O#2"), new IOException("I/O#3")))

      val result = SpecError() |+| errors

      result.toVect should have length 4
      all(result.tail) shouldBe an[IOException]

      assertThrows[SpecError] {
        result.printStackTrace()
        throw result.head
      }
    }

    scenario("a DataLakeErrors can be create from two independent errors") {

      val exception = new IllegalArgumentException("wrong argument")
      val error = SpecError()

      val result = error |+| exception

      result.toVect should have length 2

      result.head shouldBe a[SpecError]
      result.tail.head shouldBe a[IllegalArgumentException]

      a[DataLakeErrors] shouldBe thrownBy {
        throw result
      }
    }
  }
}

object ErrorSpec {

  final case class SpecError(message: String = "message") extends DataLakeTechnicalError
}
