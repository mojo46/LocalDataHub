package com.renault.datalake.dll.common.core.security

import com.renault.datalake.dll.common.core.encryption.Decryptor

/**
  * Utility class for ES configuration preparation & validation
  */
object WhiteList {

  // Indicate necessary fields and if they are encrypted
  val esConf = Map(
    "es_hosts" -> false,
    "es_port_http" -> false,
    "es_user" -> false,
    "es_password" -> true,
    "es_ssl" -> false,
    "es_ssl_truststore_path" -> false,
    "es_ssl_truststore_password" -> true,
    "es_cluster_name" -> false,
    "es_index_auto_create" -> false,
    "es_no_discovery" -> false
  )

  // DLL conf to Spark Context conf mapping
  val mappingEsToSparkContextConf = Map(
    "es_hosts" -> "es.nodes",
    "es_port_http" -> "es.port",
    "es_cluster_name" -> "es.net.http.header.X-Found-Cluster",
    "es_user" -> "es.net.http.auth.user",
    "es_password" -> "es.net.http.auth.pass",
    "es_ssl" -> "es.net.ssl",
    "es_ssl_truststore_path" -> "es.net.ssl.truststore.location",
    "es_ssl_truststore_password" -> "es.net.ssl.truststore.pass",
    "es_index_auto_create" -> "es.index.auto.create",
    "es_no_discovery" -> "es.nodes.wan.only"
  )

  // DLL conf to Elastic API conf mapping
  val mappingEsToElasticApiConf = Map(
    "es_hosts" -> "es.nodes",
    "es_cluster_name" -> "es.cluster",
    "es_port_http" -> "es.port",
    "es_user" -> "es.auth.user",
    "es_password" -> "es.auth.password",
    "es_ssl_truststore_path" -> "es.ssl.truststore.path",
    "es_ssl_truststore_password" -> "es.ssl.truststore.pass"
  )

  val hqlRegexs = List("^CREATE\\s*TEMPORARY\\s*FUNCTION\\s*[a-zA-Z0-9_]*\\s*AS\\s*\\'[a-zA-Z0-9\\.]*\\'",
    "^(?i)[ \\t]*SET[ \\t]+(\\S+)[ \\t]*(=)[ \\t]*(.+)"
  )

  val dllTechnicalFields = List("DLL_VALIDITE", "DLL_CHECKSUM", "DLL_DATE_MAJ", "DLL_DATE_CREATION")

  val kafkaConf = Map(
    "kafka.bootstrap.servers" -> false,
    "checkpointLocation" -> false,
    "kafka.security.protocol" -> false,
    "kafka.ssl.truststore.location" -> false,
    "kafka.ssl.truststore.password" -> false,
    "kafka.ssl.keystore.location" -> false,
    "kafka.ssl.keystore.password" -> false
  )

  val kafkaReaderConf = Map(
    "assign" -> false,
    "subscribe" -> false,
    "subscribePattern" -> false,
    "kafka.bootstrap.servers" -> false,
    "startingOffsets" -> false,
    "endingOffsets" -> false,
    "failOnDataLoss" -> false,
    "kafkaConsumer.pollTimeoutMs" -> false,
    "fetchOffset.numRetries" -> false,
    "fetchOffset.retryIntervalMs" -> false,
    "maxOffsetsPerTrigger" -> false
  )

  /**
    *
    * @param line [[String]] HQL statement to check
    * @return { @code true} if, and only if, the string matches all the [[WhiteList.hqlRegexs]] regular expressions
    */
  def checkAllHqlRegexs(line: String): Boolean = {
    WhiteList.hqlRegexs.
      map(pattern => line matches pattern)
      .reduceRight((a, r) => a | r)
  }

  /**
    * Validate and transform typesafe ES conf for Spark Context
    *
    * @param myConf [[Map]] of [[String]] parsed configuration
    * @return [[Map]] of [[String]] configuration
    */
  def ESConfToSparkContext(myConf: Map[String, String]): Map[String, String] = {
    filterOnWhiteListElement(myConf)
      .filter(p => mappingEsToSparkContextConf.exists(_._1 == p._1))
      .map(p => (mappingEsToSparkContextConf(p._1), if (esConf(p._1)) Decryptor.decrypt(p._2) else p._2))
  }

  /**
    * Remove configuration that are not whitelisted in esConf
    *
    * @param myConf [[Map]] of [[String]] parsed configuration
    * @return [[Map]] of [[String]] configuration
    */
  def filterOnWhiteListElement(myConf: Map[String, String]): Map[String, String] = {
    myConf.filter(p => esConf.exists(_._1 == p._1))
  }

  /**
    * Validate and transform typesafe ES conf for Elastic API
    *
    * @param myConf [[Map]] of [[String]] parsed configuration
    * @return [[Map]] of [[String]] configuration
    */
  def ESConfToElasticAPI(myConf: Map[String, String]): Map[String, String] = {
    filterOnWhiteListElement(myConf)
      .filter(p => mappingEsToElasticApiConf.exists(_._1 == p._1))
      .map(p => (mappingEsToElasticApiConf(p._1), if (esConf(p._1)) Decryptor.decrypt(p._2) else p._2))
  }

  def kafkaWhiteListProperties(myConf: Map[String, String]): Map[String, String] =
    myConf.filter(conf => kafkaConf.exists(_._1 == conf._1))

  def kafkaReaderWhiteListProperties(myConf: Map[String, String]): Map[String, String] =
    myConf.filter(conf => kafkaConf.exists(_._1 == conf._1))
}
