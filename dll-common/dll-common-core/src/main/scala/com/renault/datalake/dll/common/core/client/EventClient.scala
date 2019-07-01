package com.renault.datalake.dll.common.core.client

import com.renault.datalake.dll.common.core.exception.DataLakeGenericValidationError
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

case class EventClient() extends Logging {

  protected var options: Map[String, String] = Map.empty[String, String]
  protected var sink: String = ""
  protected var behaviour: String = ""

  def option(key: String, value: String): EventClient = {
    this.options = this.options + (key -> value)
    this
  }

  def options(options: Map[String, String]): EventClient = {
    this.options = options
    this
  }

  def sink(sink: String): EventClient = {
    this.sink = sink
    this
  }

  def behaviour(behaviour : String): EventClient ={
    this.behaviour =behaviour
    this
  }


  def build(): EventClient = {
    sink match {
      case "kafka" => new KafkaClient().behaviour(behaviour).options(options)
      case _ => throw DataLakeGenericValidationError(s"STREAM SINK '$sink' IS NOT SUPPORTED")
    }
  }

  def getData(): Seq[DataFrame] = {
    Seq.empty[DataFrame]
  }


}
