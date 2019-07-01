package com.renault.datalake.dll.common.core.exception.specific

import com.renault.datalake.dll.common.core.exception.{DataLakeError, DataLakeFunctionalError}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode.Append

import scala.util.Try

object Transformer {

  sealed abstract class TransformError extends DataLakeError

  case class InvalidCast(message: String) extends TransformError

  case class UndefinedCustomFunction() extends TransformError {
    val message = "When a custom transformer is used, 'apply' property should be defined"
  }

  case class ValidatorError(columnName: String, regex: String) extends TransformError with DataLakeFunctionalError {
    override def message = s"Some cells in the '$columnName' column didn't respect the regex: $regex"
  }

  case class ValidatorErrors(invalidFrame: DataFrame,
                             errors: Vector[ValidatorError]) extends TransformError with DataLakeFunctionalError {
    override def message: String = errors.map(_.message).mkString(System.lineSeparator())

    def writeInvalidFrame(path: Path): Try[Path] = Try {
      invalidFrame.coalesce(1)
        .write.mode(Append)
        .option("header", value = true)
        .option("codec", value = "none")
        .csv(path.toString)
      path
    }
  }
}
