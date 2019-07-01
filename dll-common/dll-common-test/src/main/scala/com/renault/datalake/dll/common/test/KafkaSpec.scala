package com.renault.datalake.dll.common.test

import java.io.File
import java.util.Properties

import com.github.sakserv.minicluster.impl.KafkaLocalBroker
import com.renault.datalake.dll.common.test.KafkaSpec.SpecTopic
import kafka.admin.AdminUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger
import org.scalatest.Suite

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait KafkaSpec extends ZookeeperLocalClusterSpec {
  this: Suite =>

  private lazy val logger = Logger.getLogger(getClass)

  private var optKafkaLocalBroker: Option[KafkaLocalBroker] = None
  private var temp: String = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    if (SystemUtils.IS_OS_WINDOWS) {
      temp = System.getProperty("java.io.tmpdir")
    } else {
      temp = "/tmp/"
    }
    logger.trace("---------------------Working Directory-----------------------" + temp)
    optKafkaLocalBroker =
      Try(new KafkaLocalBroker.Builder()
        .setKafkaHostname("localhost")
        .setKafkaPort(9092)
        .setKafkaBrokerId(0)
        .setKafkaProperties(new Properties())
        .setKafkaTempDir(temp.concat("/embedded_kafka"))
        .setZookeeperConnectionString("localhost:2181")
        .build

      ).toOption

    optKafkaLocalBroker.foreach(_.start())

    optZkUtils.foreach { zkUtils =>
      testTopics.foreach { topic =>
        AdminUtils.createTopic(
          zkUtils,
          topic.name,
          topic.partitionNumber,
          topic.replicationFactor
        )
      }
    }
  }

  override def afterAll(): Unit = {
    optKafkaLocalBroker.map { broker =>
      Try {
        broker.stop(true)
        if (new File(temp.concat("/embedded_kafka")).exists()) {
          FileUtils.deleteDirectory(new File(temp.concat("/embedded_kafka")))
         }
      } match {
        case Success(deleted) => "Delete the local kafka broker with success"
         case Failure(ex) => s"Fail with the following error: ${ex.getMessage}"
      }
    }.foreach(logger.warn)
    super.afterAll()
  }

  def testTopics: Vector[SpecTopic]
}

object KafkaSpec {

  type GenericConsumerRecords = ConsumerRecords[String, String]

  case class SpecTopic(name: String, partitionNumber: Int, replicationFactor: Int = 1)

  trait RecordFormatter[T] {
    def format[K, V](records: T): Vector[(K, V)]
  }

  implicit val kafkaRecordFormatter: RecordFormatter[GenericConsumerRecords] =
    new RecordFormatter[GenericConsumerRecords] {
      def format[K, V](records: GenericConsumerRecords): Vector[(K, V)] =
        records.asScala.map { record =>
          record.key().asInstanceOf[K] -> record.value().asInstanceOf[V]
        }.toVector
    }

  implicit class RecordFormatterSyntax[GenericConsumerRecords](value: GenericConsumerRecords) {
    def asVector[K, V](implicit formatter: RecordFormatter[GenericConsumerRecords]): Vector[(K, V)] =
      formatter.format(value)
  }

  def kafkaSpecProperties(testName: String, config: Properties = new Properties()): Properties = {
    val appId = s"$testName-${System.currentTimeMillis()}"
    config.put(ConsumerConfig.GROUP_ID_CONFIG, appId)
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, appId)
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config
  }

  def createSpecConsumer(testName: String, config: Properties = new Properties()): KafkaConsumer[String, String] = {
    val properties = kafkaSpecProperties(testName, config)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

    new KafkaConsumer[String, String](properties)
  }

  def createSpecProducer(testName: String, config: Properties = new Properties()): KafkaProducer[String, String] = {
    val properties = kafkaSpecProperties(testName, config)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    new KafkaProducer[String, String](properties)
  }
}
