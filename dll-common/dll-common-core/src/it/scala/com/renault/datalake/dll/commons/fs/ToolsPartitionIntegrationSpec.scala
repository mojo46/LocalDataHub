package com.renault.datalake.dll.commons.fs

import com.renault.datalake.dll.common.core.fs.Utils
import com.renault.datalake.dll.common.test.LocalClusterSpec
import org.apache.hadoop.fs.Path
import org.scalatest._

/**
  * ToolsPartitionIntegrationSpec
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class ToolsPartitionIntegrationSpec extends FeatureSpec with LocalClusterSpec with Matchers {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  feature("Get partitions of a given folder") {
    scenario(" Case of 3c paritions") {
      fsContext.mkdirs(new Path("/tmp/hadoop/input"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1__1"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_2/"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_2/1_1_2/"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_3/1_1_2/"))

      val result = getPartionColunmWithValues(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input"),
        Utils(fsContext).getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input")))

      result.foreach(f => println("part: " + f))
      assert(result.size == 3)
      assert(result.head.split(",").size == 3)
    }


    scenario("  Case of 5 paritions") {

      fsContext.mkdirs(new Path("/tmp/hadoop/input"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_1"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_1/1_1_1_1/1_1_1_1_1/"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_1/1_1_1_2/1_1_1_1_1/"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_1/1_1_1_3/1_1_1_1_3/"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_1/1_1_1_4/1_1_1_1_4/"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/1/1_1/1_1_1/1_1_1_5/1_1_1_1_5/"))

      val result = getPartionColunmWithValues(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input"),
        Utils(fsContext).getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input")))

      result.foreach(f => println("part: " + f))

      assert(result.size == 5)
      assert(result.head.split(",").size == 5)
    }

    scenario("  Case of 3 paritions") {
      fsContext.delete(new Path("/tmp/hadoop/input"), true)
      fsContext.mkdirs(new Path("/tmp/hadoop/input"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_2"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_2/2_2_2"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_2/2_2_3"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_2/2_2_4"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_2/2_2_5"))

      val result = getPartionColunmWithValues(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input"),
        Utils(fsContext).getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input")))

      result.foreach(f => println("part: " + f))

      assert(result.size == 4)
      assert(result.head.split(",").size == 3)
    }

    scenario("  Case of 2 paritions") {
      fsContext.delete(new Path("/tmp/hadoop/input"), true)
      fsContext.mkdirs(new Path("/tmp/hadoop/input"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_2"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_3"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_4"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_5"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2/2_6"))
      val result = getPartionColunmWithValues(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input"),
        Utils(fsContext).getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input")))

      result.foreach(f => println("part: " + f))

      assert(result.size == 5)
      assert(result.head.split(",").size == 2)
    }

    scenario("  Case of 1 paritions") {
      fsContext.delete(new Path("/tmp/hadoop/input"), true)
      fsContext.mkdirs(new Path("/tmp/hadoop/input"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/2"))
      fsContext.mkdirs(new Path("/tmp/hadoop/input/3/"))
      val result = getPartionColunmWithValues(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input"),
        Utils(fsContext).getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input")))

      result.foreach(f => println("part: " + f))

      assert(result.size == 2)
      assert(result.head.split(",").size == 1)
    }

    scenario("  Case of No paritions") {
      fsContext.delete(new Path("/tmp/hadoop/input"), true)
      fsContext.mkdirs(new Path("/tmp/hadoop/input"))
      val result = getPartionColunmWithValues(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input"),
        Utils(fsContext).getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/input")))

      result.foreach(f => println("part: " + f))

      assert(result.size == 0)
    }
  }

  def getPartionColunmWithValues(outputPath: Path, listOfSubDir: Set[Object]): Set[String] = {
    if (fsContext.exists(outputPath)) {
      var l = listOfSubDir
        .map(_.asInstanceOf[(String, Path)])
        .map(_._2.toString.stripPrefix(outputPath.toString))
      if (l.isEmpty) {
        Set()
      } else {
        val maxLength = l.reduceLeft((x, y) => if (x.length > y.length) x else y).length
        l.filter(_.length == maxLength)
          .map(_.replace("/", ","))
          .map(x => if (x.startsWith(",")) x.stripPrefix(",") else x)
      }
    }
    else Set()

  }


}
