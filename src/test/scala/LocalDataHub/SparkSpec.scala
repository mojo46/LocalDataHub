package com.renault.datalake.dlt.test

import java.io.{BufferedReader, File, InputStreamReader}

import org.apache.commons.io.{FileDeleteStrategy, FileUtils, IOUtils}
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * SparkSpec
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  lazy val logger = Logger.getLogger(this.getClass.getName)
  private var _sc: SparkSession = _
  private var hiveMetastoreHostname: String = _
  private var hiveMetastorePort: Int = _
  private var hiveMetastoreDerbyDbDir: String = _
  private var hiveScratchDir: String = _
  private var hiveWarehouseDir: String = _
  private var hiveConf: HiveConf = _
  private var hiveUris: String = _
  private var hadoopHome: String = _
  private var t: Thread = _
  protected var temp: String = _
  private var command: String = _

  override def beforeAll() {
    super.beforeAll()
    printBanner()
    startHiveMetastore()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkTest")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "127.0.0.1")
      .set("es.port", "9002")
      .set("cluster.name", "elasticsearch_test")
      .set("spark.sql.warehouse.dir","/tmp/hive/warehouse")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.orc.impl", "hive")
      .set("spark.sql.orc.enableVectorizedReader", "false")
      .set("spark.sql.hive.convertMetastoreOrc","false")
      .set("spark.deploy.zookeeper.url","127.0.0.1:2181")
      .set("spark.deploy.zookeeper.dir","embedded_zk")

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    logger.info("## Opening SparkSession")
    _sc = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate
  }

  override def afterAll() {
    if (_sc != null) {
      logger.info("## Closing SparkSession")
      _sc.stop()
      _sc = null
      stopHiveMetastore()
    }

    super.afterAll()
  }

  def sparkConfig: Map[String, String] = Map.empty


  /**
    * Start Local Hive metastore instance
    * @throws java.lang.Exception
    */
  @Override
  @throws(classOf[Exception])
  def startHiveMetastore() {
    configureMetastore()

    val startHiveLocalMetaStore: StartHiveLocalMetaStore = new StartHiveLocalMetaStore()
    startHiveLocalMetaStore.setHiveMetastorePort(hiveMetastorePort)
    startHiveLocalMetaStore.setHiveConf(hiveConf)

    t = new Thread(startHiveLocalMetaStore)
    t.setDaemon(true)
    t.start()
    logger.trace("---------------------Start META-----------------------" + t.getId)
  }

  /**
    * Set local hive metastore configuration
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def configureMetastore() {

    if (SystemUtils.IS_OS_WINDOWS) {
      temp = System.getProperty("java.io.tmpdir")
      saveAndLoadResource(new File(temp.toString + "hadoop"), "lib/hadoop.dll", true, true)
      saveAndLoadResource(new File(temp.toString + "hadoop"), "lib/hdfs.dll", true, true)
      saveAndLoadResource(new File(temp.toString + "hadoop"), "lib/libwinutils.lib", true, false)
      saveAndLoadResource(new File(temp.toString + "hadoop"), "bin/winutils.exe", true, false)
      command = temp.toString + "hadoop" + "/bin/winutils.exe chmod -R  777  " + temp.toString + "hive"
      logger.trace("---------------------command-----------------------" + command)
    } else {
      temp = "/tmp/"
      command = " chmod -R 777 /tmp/hive"
    }

    Runtime.getRuntime.exec(command)

    hiveMetastoreHostname = "localhost"
    hiveMetastorePort = 12356
    hiveWarehouseDir = temp.toString + "hive" + File.separator + "warehouse"
    hiveMetastoreDerbyDbDir = temp.toString + "hive" + File.separator + "metastore_db"
    hiveScratchDir = temp.toString + "hive"
    hiveUris = "thrift://localhost:12356"
    hadoopHome = temp.toString + "hadoop"

    logger.trace("---------------------hiveScratchDir-----------------------" + hiveScratchDir)
    logger.trace("---------------------hiveScratchDir-----------------------" + hiveWarehouseDir)
    logger.trace("---------------------hiveScratchDir-----------------------" + hiveMetastoreDerbyDbDir)

    System.setProperty("HADOOP_HOME", hadoopHome)
    System.setProperty("hadoop.home.dir", hadoopHome)
    System.setProperty("hive.exec.scratchdir", hiveScratchDir)
    System.setProperty("hive.warehouse.dir", hiveWarehouseDir)
    System.setProperty("hive.metastore.derby.db.dir", hiveMetastoreDerbyDbDir)
    System.setProperty("hive.metastore.hostname", hiveMetastoreHostname)
    System.setProperty("hive.metastore.port", hiveMetastorePort.toString)
    System.setProperty("hive.metastore.uris", hiveUris)
    System.setProperty("spark.testing", "true")

    hiveConf = new HiveConf()
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, s"thrift://$hiveMetastoreHostname:$hiveMetastorePort")
    hiveConf.setVar(HiveConf.ConfVars.SCRATCHDIR, hiveScratchDir)
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, s"jdbc:derby:;databaseName=$hiveMetastoreDerbyDbDir;create=true")
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, new File(hiveWarehouseDir).getAbsolutePath)
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true)
    hiveConf.set("datanucleus.schema.autoCreateTables", "true")
    hiveConf.set("hive.metastore.schema.verification", "false")
    hiveConf.set("hive.exec.dynamic.partition", "true")
    hiveConf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveConf.set("spark.sql.warehouse.dir","/tmp/hive/warehouse")
    hiveConf.set("spark.sql.orc.impl", "hive")
    hiveConf.set("spark.sql.orc.enableVectorizedReader", "false")
    hiveConf.set("spark.sql.hive.convertMetastoreOrc","false")

  }

  /**
    * Copy all needed libs before initialisation ol local instance of hiveContext on WinOS
    * @param outputDirectory
    * @param name
    * @param replace
    * @param load
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def saveAndLoadResource(outputDirectory: File, name: String, replace: Boolean, load: Boolean) {
    val file = new File(outputDirectory, name)
    if (!file.exists()) {

      val in = this.getClass.getClassLoader.getResourceAsStream(name)
      val out = FileUtils.openOutputStream(file)
      IOUtils.copy(in, out)
      in.close()
      out.close()
      if (load) System.load(file.getAbsolutePath)
    }
  }

  @Override
  @throws(classOf[Exception])
  def stopHiveMetastore() {
    stop(true)
    logger.trace("---------------------STOP META-----------------------")
  }

  /**
    * Stop metastore thread
    * @param isCleanUp
    * @throws java.lang.Exception
    */
  @Override
  @throws(classOf[Exception])
  def stop(isCleanUp: Boolean) {
    t.interrupt()
    /*if (isCleanUp)
      cleanUp()*/
  }

  def sc: SparkSession = _sc

  /**
    * Delete metastore repository
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def cleanUp() = {
    if (new File(hiveMetastoreDerbyDbDir).exists()) {
      new File(hiveMetastoreDerbyDbDir).listFiles().foreach { x => FileDeleteStrategy.FORCE.delete(x) }
      FileUtils.deleteDirectory(new File(hiveMetastoreDerbyDbDir))
      logger.trace("---------------------EXISTS FILE -----------------------" + hiveMetastoreDerbyDbDir)
    }
  }

  def printBanner(): Unit = {
    val in = this.getClass.getClassLoader.getResourceAsStream("name.txt")
    System.setProperty("config.resource", this.getClass.getClassLoader.getResource("reference.conf").getFile)
    val bufferedReader = new BufferedReader(new InputStreamReader(in))
    bufferedReader.lines().toArray.foreach(Console.out.println(_))
  }
}

object SparkSpec {}
