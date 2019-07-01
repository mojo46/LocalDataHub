package com.renault.datalake.dll.common.test

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.Suite

/**
  * JsonParserSpec
  * Pre-load an ObjectMapper and add utility functions
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
trait SparkWithJsonParserSpec extends SparkSpec {
  this: Suite =>

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  /**
    * Parse a JSON file from resources
    * @param path path to JSON file
    * @param valueType Object's class to parse to
    * @tparam T Class to parse to
    * @return Instance of T
    */
  def parseResource[T](path: String, valueType: Class[T]): T = mapper.readValue(
    sc.sparkContext.textFile(getClass.getClassLoader.getResource(path).getFile).collect().mkString,
    valueType
  )
}

object SparkWithJsonParserSpec {}
