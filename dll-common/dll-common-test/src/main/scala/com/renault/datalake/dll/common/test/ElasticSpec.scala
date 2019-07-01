package com.renault.datalake.dll.common.test

import java.net.URL
import java.util.concurrent.TimeUnit._

import org.scalatest.{BeforeAndAfterAll, Suite}
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}


/**
  * ElasticSpec
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
trait ElasticSpec extends BeforeAndAfterAll with SparkSpec {
  this: Suite =>

  private val elasticVersion = "6.3.1"
  var _embeddedElastic: EmbeddedElastic = _

  override def beforeAll() {
    super.beforeAll()

    _embeddedElastic = EmbeddedElastic.builder()
      .withDownloadUrl(new URL(s"http://rnexus.intra.renault.fr/rnexus/content/groups/renault-entry/com/renault/datalake/elasticsearch/$elasticVersion/elasticsearch-$elasticVersion.zip"))
      .withSetting(PopularProperties.HTTP_PORT, 9002)
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9003)
      .withSetting(PopularProperties.CLUSTER_NAME, "elasticsearch_test")
      .withStartTimeout(5, MINUTES)
      .build()
      .start()
  }


  override def afterAll(): Unit = {
    super.afterAll()

    if (_embeddedElastic != null) {
      try {
        _embeddedElastic.stop()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
