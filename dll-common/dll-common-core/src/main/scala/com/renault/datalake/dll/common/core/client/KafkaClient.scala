package com.renault.datalake.dll.common.core.client

import com.renault.datalake.dll.common.core.connector.Spark
import com.renault.datalake.dll.common.core.exception.DataLakeGenericValidationError
import org.apache.spark.sql.DataFrame

class KafkaClient() extends EventClient {

  override def getData(): Seq[DataFrame] = {

    behaviour match {
      case "batch" => {
        Seq(Spark().sparkSession.read.format("kafka").options(options).load())
      }
      case "stream" => {
        Seq(Spark().sparkSession.readStream.format("kafka").options(options).load())

      }
      case _ => throw DataLakeGenericValidationError(s"The behaviour for your stream reader" +
        s" $behaviour is not set or supported. It's either 'batch' or 'stream'")
    }
  }
}
