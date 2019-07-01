package com.renault.datalake.dll.common.core.exception

import cats.instances.vector._
import cats.syntax.applicative._

trait DataLakeError extends Throwable {

  def message: String

  override def getMessage: String = message

  def |+|(cause: Throwable): DataLakeErrors = DataLakeErrors(this, cause.pure[Vector])
  def |+|(cause: DataLakeErrors): DataLakeErrors = DataLakeErrors(this, cause.toVect)
}
