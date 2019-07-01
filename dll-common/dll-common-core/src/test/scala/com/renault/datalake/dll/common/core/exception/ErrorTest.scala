package com.renault.datalake.dll.common.core.exception

import DataLakeErrors._
import java.io.IOException

import org.apache.spark.sql.SparkSession

object ErrorTest extends App {

  val exception = new IndexOutOfBoundsException("some vector was empty")

  val firstError = new IOException("some programmatic error just occur").toErrors |+| exception

  val oneAndHalf = new UnsupportedOperationException("well, somthin went wrong", new IOException("U can't touch this", new IllegalAccessError("U have no right"))
  ).toErrors |+| firstError

  val secError = new IOException("fail to validate the the processing due to the following").toErrors |+| oneAndHalf

  val finalError = new IOException("the processing has been stopped by the following error").toErrors |+| secError

  //finalError.getCause.getCause.getMessage shouldBe "some programmatic error just occur:"

  //throw finalError

  final case class SpecError(message: String = "message") extends DataLakeTechnicalError


  println(SpecError().sparkVersion)

  println(SparkSession.getActiveSession.map(
    _.sparkContext.getExecutorMemoryStatus.toMap)
    .getOrElse(Map.empty))
}
