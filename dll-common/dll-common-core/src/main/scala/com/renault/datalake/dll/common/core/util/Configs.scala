package com.renault.datalake.dll.common.core.util

import com.typesafe.config.{Config, ConfigFactory}


/**
  * getConfig from external file
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
object Configs {

  //config file
  private var myConfig: Config = ConfigFactory.load(System.getProperty("config.resource"))
  val keyStorePass: String = "37karj-b9Nm_Kw_2WFt"

  def setConfig(newConfig: Config): Unit = {
    myConfig = newConfig
  }

  def getConfig(): Config = {
    myConfig
  }

  /**
    * getString value
    */
  def getString(key: String): String = {
    myConfig.getString(key)
  }

}
