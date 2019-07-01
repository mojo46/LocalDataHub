package com.renault.datalake.dll.commons


import com.renault.datalake.dll.common.core.connector.Spark
import com.renault.datalake.dll.common.core.exception.DataLakeGenericTechnicalError
import com.renault.datalake.dll.common.core.fs.Utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame}

import com.renault.datalake.dll.common.test.{LocalClusterSpec, SparkSqlSpec, SparkWithJsonParserSpec}
import org.scalatest.{BeforeAndAfter, FeatureSpec, Matchers}

import scala.util.Try

class LocalClusterTesting extends FeatureSpec
  with LocalClusterSpec with SparkSqlSpec
  with SparkWithJsonParserSpec with Matchers {

  override def beforeAll: Unit = {
    super.beforeAll()

  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  feature(" to tst ") {
    scenario(" to test one scenario") {
      val mkdirvar = Try(mkdir("hadoopdata"))
      println(s"mkdir => ${mkdirvar}")

      val upload = Try(uploadResource("D:\\LocalDataHub\\resources\\core_dataset_Feb.csv", "hadoopdata"))
      println(s"upload status => ${upload}")

      val files = list("hadoopdata")
      println(s"files =>  ${files.toList}")

      val files_in_path = files.map(_.getPath.toString)

      files_in_path.foreach(println)

      files_in_path.map(m=> Spark.sparkSession.read.option("header",value=true).csv(m))

      println(" printinh message ")

    }
  }

}
