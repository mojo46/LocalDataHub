package com.renault.datalake.dll.common.test


import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}

trait ZookeeperLocalClusterSpec extends BeforeAndAfterAll {
  this: Suite =>

  private lazy val logger = Logger.getLogger(getClass)

  private var optZookeeperLocalCluster: Option[ZookeeperLocalCluster] = Option.empty[ZookeeperLocalCluster]

  protected var optZkClient: Option[ZkClient] = None

  protected var optZkUtils: Option[ZkUtils] = None

  override def beforeAll(): Unit = {
    super.beforeAll()
    optZookeeperLocalCluster = Try(
      new ZookeeperLocalCluster
      .Builder()
        .setPort(2181)
        .setTempDir("embedded_zookeeper")
        .setZookeeperConnectionString("localhost:2181")
        .setMaxClientCnxns(60)
        .setElectionPort(20001)
        .setQuorumPort(20002)
        .setDeleteDataDirectoryOnClose(false)
        .setServerId(1)
        .setTickTime(2000)
        .build

    ).toOption

    optZookeeperLocalCluster.foreach(_.start())

    optZkClient = Try {
      ZkUtils.createZkClient("localhost:2181", 3.seconds.toMillis.toInt, 3.seconds.toMillis.toInt)
    }.toOption

    optZkUtils = optZkClient.map(client =>  ZkUtils(client, isZkSecurityEnabled = false))
  }

  override def afterAll(): Unit = {
    optZookeeperLocalCluster.map { zookeeper =>
      Try{
        zookeeper.stop(true)
        Path("embedded_zookeeper").delete
      } match {
        case Success(deleted) if deleted => "Delete the local zookeeper broker with success"
        case Success(deleted) if !deleted => "Fail to remove all zookeeper temporary folders"
        case Failure(ex) => s"Fail to delete the local zookeeper with error: ${ex.getMessage}"
      }
    }.foreach(logger.warn)
    super.afterAll()
  }
}
