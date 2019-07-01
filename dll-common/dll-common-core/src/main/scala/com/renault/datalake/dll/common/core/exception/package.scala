package com.renault.datalake.dll.common.core

import java.text.SimpleDateFormat

import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}
import org.apache.spark.sql.SparkSession

import scala.util.Try

package object exception {

  sealed abstract class NotUsedResult

  case object NotUsedResult extends NotUsedResult

  private[dll] trait DataLakeFunctionalError extends DataLakeManagedError
  private[dll] trait DataLakeTechnicalError extends DataLakeManagedError
  private[dll] trait DataLakeValidationError extends DataLakeManagedError

  private[dll] case class DataLakeGenericFunctionalError(message: String) extends DataLakeFunctionalError
  private[dll] case class DataLakeGenericTechnicalError(message: String) extends DataLakeTechnicalError
  private[dll] case class DataLakeGenericValidationError(message: String) extends DataLakeValidationError

  private[dll] trait DataLakeManagedError extends DataLakeError {

    override def getMessage: String = toString

    val errorMessage: String = "<session-not-available>"
    val sparkSessionOpt: Option[SparkSession] = SparkSession.getActiveSession

    val time: String = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS").format(System.currentTimeMillis())

    val sparkLang: String = "Scala"

    val sparkUser: String =
      sparkSessionOpt.flatMap(session => Try(session.sparkContext.sparkUser).toOption).getOrElse(errorMessage)

    var appId: String =
      sparkSessionOpt.flatMap(session => Try(session.sparkContext.applicationId).toOption).getOrElse(errorMessage)

    var appName: String =
      sparkSessionOpt.flatMap(session => Try(session.sparkContext.appName).toOption).getOrElse(errorMessage)

    var sparkVersion: String =
      sparkSessionOpt.flatMap(session => Try(session.sparkContext.version).toOption).getOrElse(errorMessage)

    var executorMemory: Map[String, (Long, Long)] = sparkSessionOpt
      .flatMap(session => Try(session.sparkContext.getExecutorMemoryStatus).toOption)
      .map(_.toMap)
      .getOrElse(Map.empty)

    override def toString: String = {
      ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)
    }
  }
}
