package com.renault.datalake.dll.common.core.connector

import java.net.{Authenticator, PasswordAuthentication}

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class Spark {

  var sparkSession: SparkSession = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  var fsContext: FileSystem = null
  var executionAccumulator: CollectionAccumulator[(Any, Any)] = null

  def init(mapConf: Map[String, String]): Unit = {
    val conf = getSparkConfig(mapConf)

    if (sparkSession == null) {
      sparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    }

    if (sqlContext == null) {
      sqlContext = sparkSession.sqlContext
      sqlContext.setConf("hive.exec.dynamic.partition", "true")
      sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    }

    if (sc == null) {
      sc = sparkSession.sparkContext
    }

    if (fsContext == null) fsContext = FileSystem.get(sc.hadoopConfiguration)

    if (executionAccumulator == null) executionAccumulator = sc.collectionAccumulator[(Any, Any)]("Errors")
  }

  def init(_sparkSession: SparkSession): Unit = {
    sparkSession = _sparkSession
    sc = sparkSession.sparkContext
    init(Map(): Map[String, String])
  }


  def init(_sparkSession: SparkSession, sqlc: SQLContext): Unit = {
    sparkSession = _sparkSession
    sc = sparkSession.sparkContext
    sqlContext = sqlc
    init(Map(): Map[String, String])
  }

  def init(_sparkSession: SparkSession, _sqlContext: SQLContext, _fsContext: FileSystem): Unit = {
    sparkSession = _sparkSession
    sc = sparkSession.sparkContext
    sqlContext = _sqlContext
    fsContext = _fsContext
    executionAccumulator = sc.collectionAccumulator[(Any, Any)]("Errors")
    init(Map(): Map[String, String])
  }

  def getContent(file: String): String = {
    getLines(file).mkString
  }

  def getLines(file: String): Array[String] = {
    sc.textFile(file).collect()
  }

  /**
    * get Spark Config with external properties
    *
    * @return SparkConf
    */
  def getSparkConfig(props: Map[String, String]): SparkConf = {
    val sparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstric")
      .set("es.batch.write.retry.count", "-1")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.orc.impl", "hive")
      .set("spark.sql.orc.enableVectorizedReader", "false")
      .set("spark.sql.hive.convertMetastoreOrc", "false")
    // .set("spark.sql.sources.partitionOverwriteMode","DYNAMIC")
    //add external properties
    if (props.keys.nonEmpty) {
      props.foreach { f =>
        sparkConf.set(f._1, f._2)
      }
    }

    sparkConf
  }

  def broadcastProxyConfig(props: Map[String, String], username: String, password: String) = {
    sc.broadcast(Authenticator.setDefault(new ProxyAuthenticator(username, password)))
    if (props.keys.nonEmpty) {
      props.foreach { f =>
        sc.broadcast(System.getProperties.put(f._1, f._2))
      }
    }
  }
}

class ProxyAuthenticator(user: String, password: String) extends Authenticator {
  override def getPasswordAuthentication(): PasswordAuthentication = {
    return new PasswordAuthentication(user, password.toCharArray())
  }
}

object Spark extends Spark {
  def apply(): Spark = {
    super.init(Map(): Map[String, String])
    this
  }

  def apply(map: Map[String, String]): Spark = {
    super.init(map)
    this
  }

  def apply(sparkSession: SparkSession): Spark = {
    super.init(sparkSession)
    this
  }

  def apply(sparkSession: SparkSession, sqlContext: SQLContext): Spark = {
    super.init(sparkSession, sqlContext)
    this
  }

  def apply(sparkSession: SparkSession, sqlContext: SQLContext, fileSystem: FileSystem): Spark = {
    super.init(sparkSession, sqlContext, fileSystem)
    this
  }
}
