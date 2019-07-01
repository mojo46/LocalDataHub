package com.renault.datalake.dll.common.core.reporter

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Service provider definition for [[Reporter]]
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
object ReporterProvider {
  def getReporter(name: String, conf: Any): Reporter = {
    val loader = ServiceLoader.load(classOf[ReporterProvider])
    val sLoader = loader.asScala

    sLoader filter (_.reporterName == name) match {
      case Nil => throw ReporterProviderException.unknown(name, None)

      case head :: Nil =>
        Try {
          head.prepareReporter(conf)
        } match {
          case Success(reporter) => reporter
          case Failure(throwable) => throw ReporterProviderException.wrongDefinition(name, Some(throwable))
        }

      case _: Any => throw ReporterProviderException.unknown(name, None)
    }
  }
}

trait ReporterProvider {
  def reporterName: String

  def prepareReporter(conf: Any): Reporter
}
