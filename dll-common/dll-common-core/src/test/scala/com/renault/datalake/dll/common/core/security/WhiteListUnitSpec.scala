package com.renault.datalake.dll.common.core.security


import com.renault.datalake.dll.common.core.util.Configs
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._

class WhiteListUnitSpec extends FunSuite with Matchers {

  private lazy val logger = Logger.getLogger(this.getClass.getName)

  System.setProperty("config.resource", getClass.getClassLoader.getResource("reference.conf").getFile)
  logger.info(s"encrypt.key=${Configs.getString("encrypt.key")}")

  test("Whitelist parsed typefage config") {
    val myConf = ConfigFactory.parseResources("whitelist/myCustomConf.conf")
      .getObject("elasticsearch")
      .map({ case (k, v) => (k, v.unwrapped().toString) })
      .toMap
    val myOKConf = WhiteList.filterOnWhiteListElement(myConf)

    val expectedConf = Map(
      "es_hosts" -> "127.0.0.1",
      "es_port_http" -> "9002",
      "es_user" -> "toto",
      "es_password" -> "9TW0V8NDAGNGlZuHOqVd3nsZMDAplXknlbaVFjyZVsg=",
      "es_ssl" -> "true",
      "es_ssl_truststore_path" -> "/test/truststore_path",
      "es_ssl_truststore_password" -> "9TW0V8NDAGNGlZuHOqVd3nsZMDAplXknlbaVFjyZVsg=",
      "es_cluster_name" -> "cluster_id",
      "es_index_auto_create" -> "true",
      "es_no_discovery" -> "true"
    )

    assert(myOKConf == expectedConf)
    assert(!myOKConf.contains("not_whitelisted"))
  }

  test("Prepare typesafe config for Spark Context") {
    val myConf = ConfigFactory.parseResources("whitelist/myCustomConf.conf")
      .getObject("elasticsearch")
      .map({ case (k, v) => (k, v.unwrapped().toString) })
      .toMap
    val myDecryptedConf = WhiteList.ESConfToSparkContext(myConf)

    val expectedConf = Map(
      "es.nodes" -> "127.0.0.1",
      "es.port" -> "9002",
      "es.net.http.header.X-Found-Cluster" -> "cluster_id",
      "es.net.http.auth.user" -> "toto",
      "es.net.http.auth.pass" -> "monpass",
      "es.net.ssl" -> "true",
      "es.net.ssl.truststore.location" -> "/test/truststore_path",
      "es.net.ssl.truststore.pass" -> "monpass",
      "es.index.auto.create" -> "true",
      "es.nodes.wan.only" -> "true"
    )

    assert(myDecryptedConf == expectedConf)
  }

  test("Prepare typesafe config for Elastic API") {
    val myConf = ConfigFactory.parseResources("whitelist/myCustomConf.conf")
      .getObject("elasticsearch")
      .map({ case (k, v) => (k, v.unwrapped().toString) })
      .toMap
    val myDecryptedConf = WhiteList.ESConfToElasticAPI(myConf)

    val expectedConf = Map(
      "es.nodes" -> "127.0.0.1",
      "es.port" -> "9002",
      "es.cluster" -> "cluster_id",
      "es.auth.user" -> "toto",
      "es.auth.password" -> "monpass",
      "es.ssl.truststore.path" -> "/test/truststore_path",
      "es.ssl.truststore.pass" -> "monpass"
    )

    assert(myDecryptedConf == expectedConf)
  }

  test("Whitelist HQL query RegEx") {
    import WhiteList.checkAllHqlRegexs
    val goodQueries =
      """CREATE TEMPORARY FUNCTION 849EHN8D_JNCUID AS '09887HD9.8884HJDH'
        |SET hive.exec.dynamic.partition = true
        |set system:xxx=5
        |set hiveconf:xxx=5
        |set env:xxx=5
        |set b=  ${hiveconf:b}
        |set hivevar:var1=(select b from TableB where b='2016-09-19')
        |set hivevar:var1=select b from TableB where b='2016-09-19'
        |set f =b0
        |SET origin='US'
        |set a=1""".stripMargin

    goodQueries.lines.foreach(query => assert(checkAllHqlRegexs(query), "Failed query: " + query))

    val badQueries =
      """CREATE TEMPORARY TABLE 09887HD9.9MLJN90 (col1 STRING, col2 int, col3 STRING)'
        |CREATE FUNCTION 849EHN8D_JNCUID AS '09887HD9.8884HJDH'
        |CREATE TABLE
        |CREATE EXTERNAL TABLE
        |INSERT INTO
        |set = ezrtfqd
        |set e =""".stripMargin

    badQueries.lines.foreach(query => assert(!checkAllHqlRegexs(query), "Failed query: " + query))


  }

}
