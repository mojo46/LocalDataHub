package com.renault.datalake.dll.common.core.connector

import java.io.{File, FileInputStream}
import java.security.KeyStore

import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.ssl.SSLContexts
import org.apache.http.{Header, HttpHost}
import org.apache.log4j.Logger
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

import scala.util.Try

/**
  * Elastic client singleton
  */
class Elastic {
  private lazy val logger = Logger.getLogger(this.getClass.getName)

  var client: RestHighLevelClient = _
  var properties: Map[String, String] = Map()
  val retryCount = 3

  def init(): Unit = {
    init(Map(): Map[String, String])
  }

  /**
    * Initialize Elastic [[RestHighLevelClient]] with SSL truststore & credentials if specified
    *
    * @param mapConf properties to use
    */
  def init(mapConf: Map[String, String]): Unit = {
    properties = if(mapConf.size>0) mapConf else properties
    var attempt = 0
    while (attempt < retryCount && !checkClient()) {
      if (attempt > 0) Thread.sleep(scala.math.pow(5, attempt).toInt * 1000)
      initConnection(properties)
      attempt += 1
    }
  }

  def initConnection(props: Map[String, String]): Unit = {

    //try to close an existing connection
    if (client != null) close()

    val isSecure = props.exists(_._1 == "es.ssl.truststore.path")

    val port = props("es.port").toInt
    val hosts = props("es.nodes").split(",").toList.map(host => {
      new HttpHost(host, port, if (isSecure) "https" else "http")
    })

    val lowLevelClientBuilder: RestClientBuilder = RestClient.builder(hosts: _*)

    val headers: Array[Header] = Array(new BasicHeader("X-Found-Cluster", props("es.cluster")))
    lowLevelClientBuilder.setDefaultHeaders(headers)

    if (isSecure) {
      val truststore = KeyStore.getInstance("jks")
      try {
        truststore.load(new FileInputStream(new File(props("es.ssl.truststore.path"))), props("es.ssl.truststore.pass").toCharArray)
      }
      lowLevelClientBuilder.setHttpClientConfigCallback(
        new RestClientBuilder.HttpClientConfigCallback {
          override def customizeHttpClient(httpAsyncClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
            httpAsyncClientBuilder
              .setSSLContext(
                SSLContexts.custom().loadTrustMaterial(truststore, null).build()
              )
              .setDefaultCredentialsProvider(prepareCredentialsProvider(
                props("es.auth.user"),
                props("es.auth.password"))
              )
        }
      )
    }

    client = new RestHighLevelClient(lowLevelClientBuilder)

  }

  /**
    * Prepare a [[BasicCredentialsProvider]] for given username & password
    *
    * @param user [[String]] username
    * @param pass [[String]] password
    * @return [[BasicCredentialsProvider]]
    */
  private def prepareCredentialsProvider(user: String, pass: String): BasicCredentialsProvider = {
    val provider = new BasicCredentialsProvider()

    provider.setCredentials(
      AuthScope.ANY,
      new UsernamePasswordCredentials(user, pass)
    )

    provider
  }

  def reset(): Unit = {
    properties = Map()
  }

  def close(): Unit = {
    try {
      client.close()
      client.getLowLevelClient.close()
    }
  }

  def getClient(): RestHighLevelClient = {
    logger.info("Elastic >> user of getClient()")
    if (!checkClient()) {
      close()
      client = null
      init()
    }
    client
  }

  def checkClient(): Boolean = {
    Try(client.ping()).getOrElse(false)
  }

}

object Elastic extends Elastic {
  def apply(): Elastic = {
    super.init()
    this
  }

  def apply(props: Map[String, String]): Elastic = {
    super.init(props)
    this
  }
}
