package com.renault.datalake.dll.common.test

import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path => HDFSPath}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.reflect.io.Path

/**
  * LocalClusterSpec
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
trait LocalClusterSpec extends BeforeAndAfterAll with SparkSpec {
  this: Suite =>

  val hdfsRoot = "hdfs://127.0.0.1:12345/tmp/hadoop"
  var _localCluster: HdfsLocalCluster = _
  private var _fsContext: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    _localCluster = new HdfsLocalCluster.Builder()
      .setHdfsNamenodePort(12345)
      .setHdfsNamenodeHttpPort(12341)
      .setHdfsTempDir("embedded_hdfs")
      .setHdfsNumDatanodes(1)
      .setHdfsEnablePermissions(false)
      .setHdfsFormat(true)
      .setHdfsEnableRunningUserAsProxyUser(true)
      .setHdfsConfig(new Configuration())
      .build

    logger.info("## Opening local HDFS cluster")
    _localCluster.start()
    _fsContext = _localCluster.getHdfsFileSystemHandle
  }

  override def afterAll(): Unit = {
    if (_localCluster != null) {
      try {
        logger.info("## Closing local HDFS cluster")
        _fsContext.close()
        _localCluster.stop(true)
        val path: Path = Path("embedded_hdfs")
        path.delete()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    super.afterAll()
  }

  def localCluster: HdfsLocalCluster = _localCluster

  /**
    * Upload a resource file to target directory in HDFS
    *
    * @param resource local resource path
    * @param target   HDFS target dir from root
    * @param root     HDFS root (default = {{{ hdfsRoot }}})
    */
  def uploadResource(resource: String, target: String, root: String = hdfsRoot): Unit = fsContext.copyFromLocalFile(
    new HDFSPath(resource),
    new HDFSPath(s"$root/$target")
  )

  /**
    * List a directory's content in HDFS
    *
    * @param path HDFS directory path
    * @param root HDFS root (default = {{{ hdfsRoot }}})
    * @return {{{ FileStatus }}} objects arrays
    */
  def list(path: String, root: String = hdfsRoot): Array[FileStatus] =
    fsContext.listStatus(new HDFSPath(s"$root/$path"))

  /**
    * Create a directory in HDFS
    *
    * @param path Directory to create
    * @param root HDFS root (default = {{{ hdfsRoot }}})
    * @return true if successful
    */
  def mkdir(path: String, root: String = hdfsRoot): Boolean = fsContext.mkdirs(new HDFSPath(s"$root/$path"))

  /**
    * Create a file in HDFS
    *
    * @param path File to create
    * @param root HDFS root (default = {{{ hdfsRoot }}})
    * @return true if successful
    */
  def createFile(path: String, root: String = hdfsRoot): Boolean =
    fsContext.createNewFile(new HDFSPath(s"$root/$path"))

  /**
    * Check if file exists in HDFS
    *
    * @param path File to check
    * @param root HDFS root (default = {{{ hdfsRoot }}})
    * @return true if exists
    */
  def exists(path: String, root: String = hdfsRoot): Boolean = fsContext.exists(new HDFSPath(s"$root/$path"))

  def fsContext: FileSystem = _fsContext

  /**
    * Delete a directory in HDFS
    *
    * @param path Directory to delete
    * @param root HDFS root (default = {{{ hdfsRoot }}})
    */
  def delete(path: String, root: String = hdfsRoot): Unit = fsContext.delete(new HDFSPath(s"$root/$path"), true)
}

object LocalClusterSpec {}
