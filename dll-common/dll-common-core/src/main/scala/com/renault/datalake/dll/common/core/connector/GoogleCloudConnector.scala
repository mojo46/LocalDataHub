package com.renault.datalake.dll.common.core.connector

import java.io.{File, FileInputStream}
import java.net.Authenticator

import com.renault.datalake.dll.common.core.connector.{ProxyAuthenticator, Spark}
import com.renault.datalake.dll.common.core.util.Configs
import com.renault.google.api.client.json.jackson2.JacksonFactory
import com.renault.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.renault.google.auth.oauth2.GoogleCredentials
import com.renault.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.renault.google.cloud.hadoop.fs.gcs.{GoogleHadoopFS, GoogleHadoopFileSystem}
import com.renault.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.renault.google.cloud.http.HttpTransportOptions
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

/**
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com> on 26/04/2019
  */
class GoogleCloudConnector {

  private val logger = Logger.getLogger(getClass)

  var bigQueryService: BigQuery = _
  var bigQueryJsonClient: Bigquery = _

  def setProperties(conf: Configuration, projectId: String, datasetId: String, tableId: String, gcsBucket: String): Configuration = {
    val file = new File(Configs.getString("gcp.keyFile"))
    conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    conf.set("fs.gs.project.id", projectId)
    conf.set("fs.AbstractFileSystem.gs.impl", classOf[GoogleHadoopFS].getName)
    conf.set("fs.gs.auth.service.account.json.keyfile", file.getPath)
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.INPUT_DATASET_ID_KEY, datasetId)
    conf.set(BigQueryConfiguration.INPUT_TABLE_ID_KEY, tableId)
    conf.set(BigQueryConfiguration.INPUT_PROJECT_ID_KEY, tableId)
    conf.set("mapred.bq.auth.service.account.json.keyfile", file.getPath)
    conf.set("mapred.bq.gcs.bucket", gcsBucket)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)
    conf.set("fs.gs.auth.service.account.json.keyfile", file.getPath)
    conf.set("mapred.bq.auth.service.account.json.keyfile", file.getPath)


    /* Settiings Spark Session*/

    Spark().sparkSession.conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    Spark().sparkSession.conf.set("fs.gs.project.id", projectId)
    Spark().sparkSession.conf.set("fs.AbstractFileSystem.gs.impl", classOf[GoogleHadoopFS].getName)
    Spark().sparkSession.conf.set("fs.gs.auth.service.account.json.keyfile", file.getPath)
    Spark().sparkSession.conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    Spark().sparkSession.conf.set(BigQueryConfiguration.INPUT_DATASET_ID_KEY, datasetId)
    Spark().sparkSession.conf.set(BigQueryConfiguration.INPUT_TABLE_ID_KEY, tableId)
    Spark().sparkSession.conf.set(BigQueryConfiguration.INPUT_PROJECT_ID_KEY, tableId)
    Spark().sparkSession.conf.set("mapred.bq.auth.service.account.json.keyfile", file.getPath)
    Spark().sparkSession.conf.set("mapred.bq.gcs.bucket", gcsBucket)
    Spark().sparkSession.conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)
    Spark().sparkSession.conf.set("fs.gs.auth.service.account.json.keyfile", file.getPath)
    Spark().sparkSession.conf.set("mapred.bq.auth.service.account.json.keyfile", file.getPath)
    BigQueryConfiguration.configureBigQueryInput(conf, s"""$projectId:${datasetId.toLowerCase}.${tableId.toLowerCase}""")
    conf
  }

  /**
    * Set GCP pk12 key file.
    */
  def setGcpPk12KeyFile(conf: Configuration, pk12KeyFile: String): Unit = {
    conf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
    conf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
    conf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
  }

  def init(bQueryService: BigQuery, bQueryJsonClient: Bigquery): Unit = {
    bigQueryJsonClient = bQueryJsonClient
    bigQueryService = bQueryService
    init(Spark().sc.hadoopConfiguration)
  }

  def init(conf: Configuration): Unit = {

    if (bigQueryService == null) {
      val file = new File(Configs.getString("gcp.keyFile"))
      if (Configs.getConfig().hasPath("gcp.proxyHost") && Configs.getConfig().hasPath("gcp.proxyPort")) {
        broadcastProxyConfig(Map(
          "http.proxyHost" -> Configs.getString("gcp.proxyHost"),
          "http.proxyPort" -> Configs.getString("gcp.proxyPort"),
          "https.proxyHost" -> Configs.getString("gcp.proxyHost"),
          "https.proxyPort" -> Configs.getString("gcp.proxyPort")
        ), Configs.getString("gcp.proxyUsername"), Configs.getString("gcp.proxyPassword"))

        System.setProperty("http.proxyHost", Configs.getString("gcp.proxyHost"))
        System.setProperty("http.proxyPort", Configs.getString("gcp.proxyPort"))
        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "")
        System.setProperty("jdk.http.auth.proxying.disabledSchemes", "")
      }


      System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", file.getPath)


      bigQueryService = BigQueryOptions.newBuilder
        .setCredentials(GoogleCredentials
          .fromStream(new FileInputStream(file))
          .createScoped(List(BigqueryScopes.BIGQUERY).asJava))
        .setProjectId(Configs.getString("gcp.projectId"))
        .build.getService

    }


    if (bigQueryJsonClient == null) {
      val transportOptions = bigQueryService.getOptions.getTransportOptions.asInstanceOf[HttpTransportOptions]
      val transport = transportOptions.getHttpTransportFactory.create()
      val initializer = transportOptions.getHttpRequestInitializer(bigQueryService.getOptions)
      bigQueryJsonClient = new Bigquery.Builder(transport, new JacksonFactory, initializer)
        .setApplicationName("dll")
        .build()
    }


  }

  def broadcastProxyConfig(props: Map[String, String], username: String, password: String): Unit = {
    Spark().sc.broadcast(Authenticator.setDefault(new ProxyAuthenticator(username, password)))
    if (props.keys.nonEmpty) {
      props.foreach { f =>
        Spark().sc.broadcast(System.getProperties.put(f._1, f._2))
      }
    }
  }
}

object GoogleCloudConnector extends GoogleCloudConnector {

  def apply(conf: Configuration): GoogleCloudConnector = {
    super.init(conf)
    this
  }

  def apply(): GoogleCloudConnector = {
    super.init(Spark().sc.hadoopConfiguration)
    this
  }

  def apply(bigQueryService: BigQuery, bigQueryJsonClient: Bigquery): GoogleCloudConnector = {
    super.init(bigQueryService, bigQueryJsonClient)
    this
  }
}
