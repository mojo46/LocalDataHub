package com.renault.datalake.dll.common.test

import com.github.sakserv.minicluster.impl.HbaseLocalCluster
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.scalatest.Suite

import scala.reflect.io.Path


/**
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com> on 13/11/2018
  */
trait HbaseLocalClusterSpec extends ZookeeperLocalClusterSpec{
  this: Suite =>
  var hbaseLocalCluster: HbaseLocalCluster = _
  private lazy val logger = Logger.getLogger(getClass)
  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP", "true")
    val hbaseConfiguration = new Configuration()
    hbaseLocalCluster = new HbaseLocalCluster.Builder()
      .setHbaseMasterPort(25111)
      .setHbaseMasterInfoPort(-1)
      .setNumRegionServers(2)
      .setHbaseRootDir("embedded_hbase")
      .setZookeeperPort(2181)
      .setZookeeperConnectionString("localhost:2181")
      .setZookeeperZnodeParent("/hbase")
      .setHbaseWalReplicationEnabled(false)
      .setHbaseConfiguration(hbaseConfiguration)
      .activeRestGateway()
      .setHbaseRestHost("localhost")
     .setHbaseRestPort(28000)
     .setHbaseRestReadOnly(false)
     .setHbaseRestThreadMax(100)
     .setHbaseRestThreadMin(2)
      .build()
      .build()
    logger.info("## Opening local HDFS cluster")
    hbaseLocalCluster.start()
  }

  override def afterAll(): Unit = {
    val path: Path = Path("embedded_hbase")
    path.delete()
    hbaseLocalCluster.stop(true)
    super.afterAll()
  }
}
