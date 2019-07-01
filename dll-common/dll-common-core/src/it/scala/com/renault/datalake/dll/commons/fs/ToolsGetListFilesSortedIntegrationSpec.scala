package com.renault.datalake.dll.commons.fs

import com.renault.datalake.dll.common.core.fs.Utils
import com.renault.datalake.dll.common.test.LocalClusterSpec
import org.apache.hadoop.fs.Path
import org.scalatest.FeatureSpec

/**
  * ToolsGetListFilesSortedIntegrationSpec
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class ToolsGetListFilesSortedIntegrationSpec extends FeatureSpec with LocalClusterSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  feature("Get the list of files in a folder given a specific order") {
    scenario("getListFilesSorted - Sorted by file name ASC") {
      fsContext.mkdirs(new Path("/tmp/hadoop/utils/1/"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/a.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/b.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/c.csv"))

      val result = Utils(fsContext).list("/tmp/hadoop/utils", "each:file_name:asc",true)
        .map(x => x.split("/")(7))
      val expected_result = Seq("a.csv", "b.csv", "c.csv")
      assert(result == expected_result)

    }

    scenario("getListFilesSorted - Sorted by file name DESC") {
      fsContext.mkdirs(new Path("/tmp/hadoop/utils/1/"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/a.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/b.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/c.csv"))

      val result =  Utils(fsContext).list("/tmp/hadoop/utils/1/", "each:file_name:desc",true).map(x => x.split("/")(7))
      val expected_result2 = Seq("c.csv", "b.csv", "a.csv")
      assert(result.equals(expected_result2))


    }

    scenario("getListFilesSorted - Sorted by last modification date  ASC") {
      fsContext.mkdirs(new Path("/tmp/hadoop/utils/1/"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/a.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/b.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/c.csv"))

      val result =  Utils(fsContext).list("/tmp/hadoop/utils/1/", "each:last_modified_date:asc",true).map(x => x.split("/")(7))
      val expected_result = Seq("a.csv", "b.csv", "c.csv")
      assert(result.equals(expected_result))

    }

    scenario("getListFilesSorted - Sorted by last modification date  DESC") {
      fsContext.mkdirs(new Path("/tmp/hadoop/utils/1/"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/a.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/b.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/c.csv"))

      val result =  Utils(fsContext).list("/tmp/hadoop/utils/1/", "each:last_modified_date:desc",true).map(x => x.split("/")(7))
      val expected_result = Seq("c.csv", "b.csv", "a.csv")
      assert(result.equals(expected_result))

    }

    scenario("getListFilesSorted - Sorted by whatever (each)") {
      fsContext.mkdirs(new Path("/tmp/hadoop/utils/1/"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/a.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/b.csv"))
      fsContext.createNewFile(new Path("/tmp/hadoop/utils/1/c.csv"))

      val result =  Utils(fsContext).list("/tmp/hadoop/utils/1/", "eachblablablabla",true)
      assert(result.length.equals(3))

    }
  }

  override def afterAll: Unit = {
    super.afterAll()
  }


}
