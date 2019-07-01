package com.renault.datalake.dll.commons.tools

import com.renault.datalake.dll.common.test.{HbaseLocalClusterSpec, LocalClusterSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.scalatest.FeatureSpec

class HbaseLocalClusterIntegrationSpec extends FeatureSpec with LocalClusterSpec
  with HbaseLocalClusterSpec {


  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  feature("Test") {

    scenario("Test1") {
      val tableName = "tableFoo"
      val colFamName = "colFamBar"
      val colQualiferName = "colQualBaz"
      val numRowsToPut = 5
      val configuration = hbaseLocalCluster.getHbaseConfiguration
      deleteHbaseTable(tableName, configuration);

      createHbaseTable(tableName, colFamName, configuration)

      for (a <- 0 to 5) {
        putRow(tableName, colFamName, String.valueOf(a), colQualiferName, "row_" + a, configuration);
      }

      for (i <- 0 to 5) {
        val result = getRow(tableName, colFamName, String.valueOf(i), colQualiferName, configuration);
        println("row_" + i, new String(result.value()))
      }
      assert(true)
    }
  }

  @throws[Exception]
  private def createHbaseTable(tableName: String, colFamily: String, configuration: Configuration): Unit = {
    val admin = new HBaseAdmin(configuration)
    val hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    val hColumnDescriptor = new HColumnDescriptor(colFamily)
    hTableDescriptor.addFamily(hColumnDescriptor)
    admin.createTable(hTableDescriptor)
  }

  @throws[Exception]
  private def deleteHbaseTable(tableName: String, configuration: Configuration): Unit = {
    val admin = new HBaseAdmin(configuration)
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }

  @throws[Exception]
  private def putRow(tableName: String, colFamName: String, rowKey: String, colQualifier: String, value: String, configuration: Configuration): Unit = {
    val table = new HTable(configuration, tableName)
    val put = new Put(Bytes.toBytes(rowKey))
    put.add(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier), Bytes.toBytes(value))
    table.put(put)
    table.flushCommits()
    table.close()
  }

  @throws[Exception]
  private def getRow(tableName: String, colFamName: String, rowKey: String, colQualifier: String, configuration: Configuration) = {
    val table = new HTable(configuration, tableName)
    val get = new Get(Bytes.toBytes(rowKey))
    get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier))
    get.setMaxVersions(1)
    table.get(get)
  }
}
