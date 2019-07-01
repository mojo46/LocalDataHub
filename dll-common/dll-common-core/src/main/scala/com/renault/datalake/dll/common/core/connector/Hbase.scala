package com.renault.datalake.dll.common.core.connector

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}

class Hbase {

  var conf : HBaseConfiguration= _
  var connection : Connection =_
  var admin :Admin=_

  def init(): Unit = {
    connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
    admin = connection.getAdmin()
  }

  def init(inputConf: HBaseConfiguration, iConnection: Connection): Unit = {
    conf = inputConf
    connection = iConnection
    admin = iConnection.getAdmin()
  }

  def init(inputConf: HBaseConfiguration, iConnection: Connection, inputAdmin: Admin): Unit = {
    conf = inputConf
    connection = iConnection
    admin = inputAdmin
  }
}

object Hbase extends Hbase {

  def apply(): Hbase = {
    super.init()
    this
  }

  def apply(inputConf: HBaseConfiguration, iConnection: Connection): Hbase = {
    super.init(inputConf, iConnection)
    this
  }

  def apply(inputConf: HBaseConfiguration, iConnection: Connection, inputAdmin: Admin): Hbase = {
    super.init(inputConf, iConnection, inputAdmin)
    this
  }
}
