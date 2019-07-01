package com.renault.datalake.dll.commons.fs

import com.renault.datalake.dll.common.core.fs.Utils
import com.renault.datalake.dll.common.test.{LocalClusterSpec, SparkSqlSpec}
import org.apache.hadoop.fs.Path
import org.scalatest.{FeatureSpec, Matchers}

import scala.io.Source

class UtilsIntegrationSpec extends FeatureSpec with SparkSqlSpec with LocalClusterSpec with Matchers {
  var utils: Utils = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    utils = Utils(fsContext)
  }

  override def afterAll: Unit = {
    super.afterAll()
  }

  feature("Get the list of resources on in a folder given a specific order") {
    scenario("Sorted by file name ASC") {
      val dir = "/list/1"
      val options = "each:file_name:asc"
      val files = List("a.csv", "b.csv", "c.csv")

      mkdir(dir)
      files.foreach(f => createFile(s"$dir/$f"))

      utils.list(s"$hdfsRoot$dir", options, recursiveness = true)
        .map(p => p.split("/").last) should equal(files)

      utils.list(s"$hdfsRoot$dir/*.csv", options, recursiveness = true)
        .map(p => p.split("/").last) should equal(files)

      utils.list(s"$hdfsRoot/list/*/*.csv", options, recursiveness = true)
        .map(p => p.split("/").last) should equal(files)
    }

    scenario("Sorted by file name ASC from multiple paths") {
      val dir = "/list"
      val src = List(s"$dir/1", s"$dir/2")
      val options = "each:file_name:asc"
      val files = List("a.csv", "b.csv", "c.csv", "d.csv", "e.csv", "f.csv")

      src.foreach(mkdir(_))
      files.take(3).foreach(f => createFile(s"${src.head}/$f"))
      files.takeRight(3).foreach(f => createFile(s"${src.last}/$f"))

      mkdir(dir)
      files.foreach(f => createFile(s"$dir/$f"))

      println(utils.list(src.map(s => s"$hdfsRoot$s"), options, recursiveness = true))

      utils.list(src.map(s => s"$hdfsRoot$s"), options, recursiveness = true)
        .map(p => p.split("/").last) should equal(files)
    }

    scenario("Sorted by file name desc with nested files") {
      val root = "/listNested"
      val dir = "1"
      val subdir = "1.1"
      val options = "each:file_name:asc"
      val files = List("a.csv", "b.csv", "c.csv")
      val expected = files.map(f => s"$subdir/$f") ++ files.map(f => s"$dir/$f")

      mkdir(s"$root/$dir")
      mkdir(s"$root/$dir/$subdir")
      files.foreach(f => createFile(s"$root/$dir/$f"))
      files.foreach(f => createFile(s"$root/$dir/$subdir/$f"))

      utils.list(s"$hdfsRoot$root/$dir", options, recursiveness = true)
          .map(p => p.split("/").takeRight(2).mkString("/")) should equal (expected)
    }
  }

  feature("Get hdfs file content as String") {
    scenario("Retrieve a simple csv reader json") {
      val root = "/read"
      val file = "sample.json"

      mkdir(root)
      uploadResource(file, root)

      utils.getFileAsString(s"$hdfsRoot$root/sample.json") should equal (Source.fromFile(
        getClass.getClassLoader.getResource(file).getFile
      ).mkString)
    }
  }

  feature("Move files") {

    scenario("Move single file") {
      val src = "/42"
      val dst = "/43"

      createFile(src)
      assert(exists(src))

      utils.move(new Path(s"$hdfsRoot$src"), new Path(s"$hdfsRoot$dst"))

      assert(!exists(src))
      assert(exists(dst))
    }

    scenario("Move multiple files") {
      val src = List("/tmp/hadoop/42", "/tmp/hadoop/43")
    }

    scenario("Move files with glob pattern") {
      val root = "/moveWithGlob"
      val src = s"$root/src"
      val dst = s"$root/dst"
      val files = List(1, 2, 3, 4)

      mkdir(src)

      files.foreach(f => createFile(s"$src/$f"))

      utils.move(s"$hdfsRoot$src/*", s"$hdfsRoot/$dst")

      assert(!files.forall(f => exists(s"$src/$f")))
      assert(files.forall(f => exists(s"$dst/$f")))
    }

    scenario("Move from multiple paths") {
      val root = "/moveMultiplePaths"
      val src = List(s"$root/src1", s"$root/src2")
      val dst = s"$root/dst"
      val files = List(1, 2, 3, 4)

      src.foreach(mkdir(_))
      for (s <- src; f <- files) yield createFile(s"$s/${s.split("/").last}_$f")

      utils.move(src.map(s => s"$hdfsRoot$s"), s"$hdfsRoot$dst")

      assert(!src.forall(s => files.forall(f => exists(s"$s/${s.split("/").last}_$f"))))
      assert(src.forall(s => files.forall(f => exists(s"$dst/${s.split("/").last}_$f"))))
    }

    scenario("Move directories preserving all the hierarchy") {
      val root = "/moveDirsPreserveHierarchy"
      val src = s"$root/src"
      val dst = s"$root/dst"
      val dirs = List(1, 2, 3)

      mkdir(src)
      mkdir(dst)
      dirs.foreach(d => {
        mkdir(s"$src/$d")
        createFile(s"$src/$d/$d")
      })

      utils.move(new Path(s"$hdfsRoot/$src"), new Path(s"$hdfsRoot/$dst"))

      dirs.forall(d => fsContext.isFile(new Path(s"$hdfsRoot$dst/$d/$d"))) should be(true)
      dirs.forall(d => exists(s"$src/$d/$d")) should be(false)
    }


    scenario("Move directories preserving hierarchy with wildcard") {
      val root = "/moveDirsWithWildcard"
      val src = s"$root/src"
      val dst = s"$root/dst"
      val prefix = "prefix"
      val prefixBad = "dont_move"
      val files = List(1, 2, 3, 4)

      mkdir(src)
      createFile(s"$src/$prefixBad")
      files.foreach(f => createFile(s"$src/$prefix%d".format(f)))

      utils.move(s"$hdfsRoot$src/$prefix*", s"$hdfsRoot$dst")

      files.forall(f => !exists(s"$src/$prefix%d".format(f)) && exists(s"$dst/$prefix%d".format(f))) should be(true)
      exists(s"$src/$prefixBad") && !exists(s"$dst/$prefixBad") should be(true)
    }

    scenario("Move directories preserving hierarchy with glob pattern and nested structure") {
      val root = "/moveDirsWithGlobAndNestedStructure"
      val src = s"$root/src"
      val dst = s"$root/dst"
      val tmp = "tmp"
      val file = "a/b/file.csv"
      val dirs = List(1, 2, 3, 4)

      mkdir(src)
      mkdir(dst)

      dirs.foreach(d => createFile(s"$src/$tmp%d/$file".format(d)))

      utils.move(s"$hdfsRoot$src/$tmp*", s"$hdfsRoot$dst")

      assert(dirs.forall(d => !exists(s"$src/$tmp%d/$file".format(d)) && exists(s"$dst/$tmp%d/$file".format(d))))
    }

    scenario("More testing") {
      val root = "/moreTesting"
      val src = s"$root/src"
      val dst = s"$root/dst"
      val dirs = List(1, 2, 3)
      val files = List("A", "B", "C", "D")

      mkdir(src)
      mkdir(dst)

      for(d <- dirs; f <- files) yield createFile(s"$src/$d/$f")

      utils.move(s"$hdfsRoot$src", s"$hdfsRoot$dst")

      assert(dirs.forall(d => files.forall(f => !exists(s"$src/$d/$f") && exists(s"$dst/$d/$f"))))
    }

    scenario("Even more testing") {
      val root="/evenMoreTesting"
      val file = "file.csv"
      val src = s"$root/$file"
      val dst = s"$root/dst"

      mkdir(root)
      createFile(src)

      utils.move(s"$hdfsRoot/$src", s"$hdfsRoot/$dst")

      assert(!exists(src))
      assert(exists(s"$dst/$file"))
      assert(fsContext.isDirectory(new Path(s"$hdfsRoot$dst")))
      assert(!fsContext.isDirectory(new Path(s"$hdfsRoot$dst/$file")) && exists(s"$dst/$file"))
    }
  }

  feature("Copy files") {

    scenario("Copy single file") {
      val root = "/copySingleFile"
      val src = s"$root/copy"
      val dst = s"$root/paste"

      createFile(src)
      utils.copyPaste(s"$hdfsRoot$src", s"$hdfsRoot$dst")

      assert(exists(src))
      assert(exists(dst))
    }

    scenario("Copy files with glob pattern") {
      val root = "/copyWithGlob"
      val src = s"$root/copy"
      val dst = s"$root/paste"
      val files = List(1, 2, 3, 4)

      mkdir(src)
      files.foreach(f => createFile(s"$src/$f"))

      utils.copyPaste(new Path(s"$hdfsRoot$src/*"), new Path(s"$hdfsRoot$dst"))

      assert(files.forall(f => exists(s"$src/$f")))
      assert(files.forall(f => exists(s"$dst/$f")))
    }

    scenario("Copy directories preserving all the hierarchy") {
      val root = "/copyWithHierarchy"
      val src = s"$root/copy"
      val dst = s"$root/paste"
      val dirs = List(1, 2, 3)

      mkdir(src)
      mkdir(dst)
      dirs.foreach(d => createFile(s"$src/$d/$d"))

      utils.copyPaste(new Path(s"$hdfsRoot$src"), new Path(s"$hdfsRoot$dst"))

      assert(dirs.forall(d => fsContext.isFile(new Path(s"$hdfsRoot$dst/$d/$d")) && exists(s"$dst/$d/$d")))
    }

    scenario("Copy from multiple sources") {
      val root = "/copyWithGlob"
      val src = s"$root/copy"
      val dst = s"$root/paste"
      val dirs = List("a", "b")
      val files = List(1, 2, 3, 4)

      mkdir(src)
      dirs.foreach(d => mkdir(s"$src/$d"))
      for (d <- dirs; f <- files) yield createFile(s"$src/$d/${d}_$f")

      utils.copyPaste(dirs.map(s => s"$hdfsRoot$src/$s"), s"$hdfsRoot$dst")

      assert(dirs.forall(d => files.forall(f => exists(s"$src/$d/${d}_$f"))))
      assert(dirs.forall(d => files.forall(f => exists(s"$dst/${d}_$f"))))
    }


    scenario("merge 2 folders") {
      val root = "/copyWithGlob"
      val src = s"$root/current"
      val src2 = s"$root/new"
      val dst = s"$root/paste"
      val dirs = List("a", "b")
      val files = List(1, 2, 3, 4)

      mkdir(src)
      mkdir(src2)
      dirs.foreach(d => mkdir(s"$src/$d"))
      for (d <- dirs; f <- files) yield createFile(s"$src/$d/${d}_$f")

      List("a", "c","d").foreach(d => mkdir(s"$src2/$d"))
      for (d <- List("a", "c","d"); f <- List(4,5,6,7)) yield createFile(s"$src2/$d/${d}_$f")

      utils.mergeFolder(new Path(s"$hdfsRoot$src"),new Path(s"$hdfsRoot$src2"), s"$hdfsRoot$dst",
        sc.sparkContext.hadoopConfiguration)

      utils.list(s"$hdfsRoot$dst","each",true) should be equals  List("hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_5",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_7",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_1",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_2",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_3",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_4",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_5",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_7",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_4",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_5",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_7"
      )

    }


       scenario("merge 2 folder with empty source folder") {
      val root = "/copyWithGlob"
      val src = s"$root/current"
      val src2 = s"$root/new"
      val dst = s"$root/paste"
      val dirs = List("a", "b")
      val files = List(1, 2, 3, 4)

      // mkdir(src)
      mkdir(src2)

      List("a", "c","d").foreach(d => mkdir(s"$src2/$d"))
      for (d <- List("a", "c","d"); f <- List(4,5,6,7)) yield createFile(s"$src2/$d/${d}_$f")

      utils.mergeFolder(new Path(s"$hdfsRoot$src"),new Path(s"$hdfsRoot$src2"), s"$hdfsRoot$dst",
        sc.sparkContext.hadoopConfiguration)

      utils.list(s"$hdfsRoot$dst","each",true) should be equals  List("hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_5",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_7",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_5",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/c_7",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_4",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_5",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/d_7"
      )

    }

    scenario("merge 2 folder with empty source1 folder") {
      val root = "/copyWithGlob"
      val src = s"$root/current"
      val src2 = s"$root/new"
      val dst = s"$root/paste"
      val dirs = List("a", "b")
      val files = List(1, 2, 3, 4)

      mkdir(src)
    //  mkdir(src2)

      dirs.foreach(d => mkdir(s"$src/$d"))
      for (d <- dirs; f <- files) yield createFile(s"$src/$d/${d}_$f")

      utils.mergeFolder(new Path(s"$hdfsRoot$src"),new Path(s"$hdfsRoot$src2"), s"$hdfsRoot$dst",
        sc.sparkContext.hadoopConfiguration)

      utils.list(s"$hdfsRoot$dst","each",true) should be equals  List("hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_1",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_2",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_3",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/a_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_1",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_2",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_3",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/b_4"
      )

    }


    scenario("merge 2 folder with many subdir") {
      val root = "/copyWithGlob"
      val src = s"$root/current"
      val src2 = s"$root/new"
      val dst = s"$root/paste"
      val dirs = List("b")
      val subdirs = List("aa","cc","dd")
      val files = List(1, 2, 3, 4)

      mkdir(src)
      mkdir(src+"/b")
      mkdir(src+"/b/bb")
      mkdir(src2)
      for (d <-  List("b/bb"); f <- files) yield createFile(s"$src/$d/${d}_$f")

      List("a", "c","d").foreach(d => mkdir(s"$src2/$d"))
      List("a/aa","c/cc","d/dd").foreach(d => mkdir(s"$src2/$d"))

      for (d <- List("a/aa", "c/cc","d/dd"); f <- List(4,5,6,7)) yield createFile(s"$src2/$d/${d}_$f")

      utils.mergeFolder(new Path(s"$hdfsRoot$src"),new Path(s"$hdfsRoot$src2"), s"$hdfsRoot$dst",
        sc.sparkContext.hadoopConfiguration)

      utils.list(s"$hdfsRoot$dst","each",true).foreach(println(_))

      utils.list(s"$hdfsRoot$dst","each",true) should be equals  List("hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/aa/a_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/aa/a_5",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/aa/a_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/a/aa/a_7",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/bb/b_1",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/bb/b_2",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/bb/b_3",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/b/bb/b_4",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/cc/c_4",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/cc/c_5",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/cc/c_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/c/cc/c_7",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/dd/d_4",
        " hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/dd/d_5",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/dd/d_6",
        "hdfs://127.0.0.1:12345/tmp/hadoop/copyWithGlob/paste/d/dd/d_7"
      )

    }

  }

  feature("to test the back up files function")
  {
    scenario("test with backupversion 1 ")
    {
      mkdir("bkptest/")
      mkdir("backup/")
      mkdir("bkptest/test/")
      mkdir("bkptest/test/state=solid")
      mkdir("bkptest/test/state=liquid")

      mkdir("backup/20160311121212/")
      mkdir("backup/20160311121212/state=solid")
      mkdir("backup/20160311121212/state=liquid")
      mkdir("backup/20160312121212/")

      uploadResource("sample.json","backup/20160312121212/")
      uploadResource("test.json","backup/20160312121212/")
      uploadResource("sample.json","backup/20160311121212/state=solid/")
      uploadResource("sample.json","backup/20160311121212/state=liquid/")

      uploadResource("sample.json","bkptest/test/state=solid/")
      uploadResource("sample.json","bkptest/test/state=liquid/")
      utils.backupFiles("hdfs://127.0.0.1:12345/tmp/hadoop/bkptest/test","hdfs://127.0.0.1:12345/tmp/hadoop/backup/",2)
      val hdfslist =  utils.fileSystem.listStatus(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/backup/")).toList
      hdfslist.foreach(p=>println("BACKUP"+p.getPath.toString))
      hdfslist.size should equal(2)
    }
    scenario("test with backupversion 2 ")
    {
      mkdir("bkptest/20160313020202/")
      uploadResource("sample.json","bkptest/20160313020202/")
      utils.backupFiles("hdfs://127.0.0.1:12345/tmp/hadoop/bkptest/20160313020202","hdfs://127.0.0.1:12345/tmp/hadoop/backup/",1)
      val hdfslist =  utils.fileSystem.listStatus(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/backup/")).toList
      hdfslist.foreach(p=>println("BACKUP2"+p.getPath.toString))
      hdfslist.size should equal(2)
    }
  }
  feature("to test the uncompress")
  {
    scenario("test for unzip for zip ")
    {
      mkdir("/compress")
      mkdir("/uncompress")
      mkdir("/archive")
      uploadResource("compresstest.zip","/compress/")
      utils.unCompressFiles(fsContext,"zip","hdfs://127.0.0.1:12345/tmp/hadoop/compress/","hdfs://127.0.0.1:12345/tmp/hadoop/uncompress","hdfs://127.0.0.1:12345/tmp/hadoop/archive")
      val files = utils.getFilesFromPath( "hdfs://127.0.0.1:12345/tmp/hadoop/uncompress/",true)
      files.foreach(p => println("outputfiles==>" + p.getPath))
      files.size should equal (3)
    }
    scenario("test for unzip for tar")
    {
      mkdir("/compress")
      mkdir("/uncompress")
      mkdir("/archive")
      uploadResource("compresstest.tar","/compress/")
      //uploadResource("test.zip","/compress/")
      utils.unCompressFiles(fsContext,"tar","hdfs://127.0.0.1:12345/tmp/hadoop/compress/","hdfs://127.0.0.1:12345/tmp/hadoop/uncompress","hdfs://127.0.0.1:12345/tmp/hadoop/archive")
      val files = utils.getFilesFromPath( "hdfs://127.0.0.1:12345/tmp/hadoop/uncompress/compresstest",true)
      files.foreach(p => println("outputfiles==>" + p.getPath))
      files.size should equal (3)
    }
    scenario("test for unzip for tar with provided as one file")
    {
      mkdir("/compress1")
      mkdir("/compress1/input/")
      mkdir("/compress1/compresstest.zip")
      mkdir("/compress1/compresstest_archive")

      uploadResource("compresstest.zip","/compress1/input")
      //uploadResource("test.zip","/compress/")
      utils.unCompressFiles(fsContext,"zip","hdfs://127.0.0.1:12345/tmp/hadoop/compress1/input/compresstest.zip","hdfs://127.0.0.1:12345/tmp/hadoop/compress1/compresstest.zip","hdfs://127.0.0.1:12345/tmp/hadoop//compress1/compresstest_archive")
      val files = utils.getFilesFromPath( "hdfs://127.0.0.1:12345/tmp/hadoop/compress1/compresstest.zip",true)
      files.foreach(p => println("outputfiles==>" + p.getPath))
      files.size should equal (3)
    }
    scenario("test for unzip for tgz with provided as one file")
    {
      mkdir("/compress")
      mkdir("/compress/input/")
      mkdir("/uncompress")
      mkdir("/compress/vehicules.tgz_TMP")
      mkdir("/compress/vehicules.tgz")

      uploadResource("vehicules.tgz","/compress/input")
      //uploadResource("test.zip","/compress/")
      val filelist = utils.unCompressFiles(fsContext,"tgz","hdfs://127.0.0.1:12345/tmp/hadoop/compress/input/vehicules.tgz","hdfs://127.0.0.1:12345/tmp/hadoop/compress/vehicules.tgz","hdfs://127.0.0.1:12345/tmp/hadoop//compress/vehicules.tgz_TMP")
      filelist.map(_.getPath).foreach(println(_))
      filelist.size should equal(1)
    }
    scenario("Delete Empty HDFS Directories")
    {
      mkdir ("DeleteEmptyDir")
      mkdir("DeleteEmptyDir/subdir1")
      mkdir("DeleteEmptyDir/subdir2")
      uploadResource("vehicules.tgz","DeleteEmptyDir/subdir1/")
      uploadResource("vehicules.tgz","DeleteEmptyDir")

      utils.deleteEmptyDirectories("hdfs://127.0.0.1:12345/tmp/hadoop/DeleteEmptyDir")
      utils.getListOfSubDir(new Path("hdfs://127.0.0.1:12345/tmp/hadoop/DeleteEmptyDir")).size should be (1)
      val files1 = utils.getFilesFromPath("hdfs://127.0.0.1:12345/tmp/hadoop/DeleteEmptyDir",true)
      files1.size should be (2)
    }
    scenario("Delete kjjjjjHDFS Directories")
    {
      mkdir ("DeleteEmptyDir2")
      mkdir("DeleteEmptyDir3")
      uploadResource("vehicules.tgz","DeleteEmptyDir3")

      utils.flatMove("hdfs://127.0.0.1:12345/tmp/hadoop/DeleteEmptyDir2",
        "hdfs://127.0.0.1:12345/tmp/hadoop/DeleteEmptyDir3",
        false,false)
    }


  }
}
