package com.renault.datalake.dll.common.core.exception

import java.io.{PrintStream, PrintWriter}

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.Try

final case class DataLakeErrors(head: Throwable, tail: Vector[Throwable]) extends DataLakeError {

  def toVect: Vector[Throwable] = head +: tail

  override def message: String = s"${tail.size + 1} errors accumulated with the first being" +
    s" - ${head.getClass.getSimpleName} -  ${head.getMessage}"

  def lastOption: Option[Throwable] = Try(tail.last).toOption

  def rootOption: Option[Throwable] = lastOption.orElse(Some(head)).map(ExceptionUtils.getRootCause)

  override def |+|(cause: Throwable) = DataLakeErrors(head, tail :+ cause)
  override def |+|(cause: DataLakeErrors) = DataLakeErrors(head, tail ++ cause.toVect)

  def causeOption: Option[Throwable] = Option(head.getCause).orElse(tail.headOption)

  override def printStackTrace(): Unit = {
    System.err.println(s"${getClass.getCanonicalName}: $getLocalizedMessage")
    super.getStackTrace.foreach(trace => System.err.println(s"\tat $trace"))
    System.err.println(s"Containing the following errors:")
    head printStackTrace()
    tail foreach { error =>
      System.err.println(s"Followed by: ${error.getClass}: ${error.getMessage}")
      error.printStackTrace()
    }
  }

  override def printStackTrace(s: PrintStream): Unit = {
    s.print(s"${getClass.getCanonicalName}: $getLocalizedMessage")
    super.getStackTrace.foreach(trace => s.print(s"\tat $trace"))
    System.err.println(s"Containing the following errors:")
    head printStackTrace s
    tail foreach { error =>
      System.err.println(s"Followed by: ${error.getClass}: ${error.getMessage}")
      error.printStackTrace(s)
    }
  }

  override def printStackTrace(w: PrintWriter): Unit = {
    w.write(s"${getClass.getCanonicalName}: $getLocalizedMessage")
    super.getStackTrace.foreach(trace => w.write(s"\tat $trace"))
    System.err.println(s"Containing the following errors:")
    head printStackTrace w
    tail foreach { error =>
      System.err.println(s"Followed by: ${error.getClass}: ${error.getMessage}")
      error.printStackTrace(w)
    }
  }

  override def getStackTrace: Array[StackTraceElement] =
    super.getStackTrace ++ head.getStackTrace ++ tail.toArray.flatMap(_.getStackTrace)

  override def getLocalizedMessage: String = message
}


object DataLakeErrors {

  implicit class ErrorOps(value: Throwable) {
    def toErrors: DataLakeErrors = DataLakeErrors(value, Vector.empty)
  }
}
