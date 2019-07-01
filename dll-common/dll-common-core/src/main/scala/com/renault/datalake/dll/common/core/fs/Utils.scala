package com.renault.datalake.dll.common.core.fs

import java.io.{BufferedInputStream, FileNotFoundException, IOException, InputStream}
import java.text.SimpleDateFormat
import java.util.zip.{ZipEntry, ZipInputStream}
import java.util.{Calendar, Date}

import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

/**
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  *
  *         This object represent the source code of the different basic features using the hadoop filesystem API
  * @param fileSystem The Hadoop FileSystem
  */
case class Utils(fileSystem: FileSystem) {
  val BUFFER: Int = 2048
  val buffer = new Array[Byte](1024)
  private lazy val logger = Logger.getLogger(this.getClass.getName)
  /**
    * Delete the desire resource on hdfs
    *
    * @param resourcePath  The filepath of the desire resource to be deleted
    * @param recursiveness In cas of deleting a directory has to set to true , for a file can be set to true or false
    */
  def delete(resourcePath: String, recursiveness: Boolean): Boolean = {
    if (fileSystem.exists(new Path(resourcePath))) {
      //return true or false if an error appears on delete
      fileSystem.delete(new Path(resourcePath), recursiveness)
    } else {
      //return true if we try to delete on a ressource that does not exist
      true
    }
  }

  /**
    * Create a directory on hdfs
    *
    * @param filePath The filepath of the desire directory to be created
    * @return true if dir creation is successful else false
    */
  def createDirectory(filePath: String): Boolean = {
    fileSystem.mkdirs(new Path(filePath))
  }

  /**
    * Move files from multiple hdfs location to one specific (can't be used for renaming ressources!)
    * Wrapper on [[move(String, String)]]
    *
    * @param src Multiple source resources
    * @param dst The destination of the resource
    * @return true if all moves are successful else false
    */
  def move(src: List[String], dst: String): Boolean = {
    src.forall(s => move(new Path(s), new Path(dst)))
  }

  /**
    * Move a file from one hdfs location to another (can be use for renaming ressources)
    * Wrapper on [[move(Path, Path)]]
    *
    * @param src The source resource
    * @param dst The destination of the resource
    * @return true if the move is successful else false
    */
  def move(src: String, dst: String): Boolean = {
    val (hsrc, hdst) = (new Path(src), new Path(dst))
    move(hsrc, hdst)
  }

  /**
    * Move a file from one hdfs location to another (can be use for renaming ressources)
    *
    * @param src [[Path]] source
    * @param dst [[Path]] destination
    * @return true if successful
    */
  def move(src: Path, dst: Path): Boolean = {

    Try {
      if (!isGlob(src) && !fileSystem.exists(src)) {
        throw new UnsupportedOperationException("Source path does not exist: " + src.toString)
      } else {
        val filesListWithoutWildCd = fileSystem.globStatus(src)

        filesListWithoutWildCd.map(
          folder => {
            var filesArraySrc = Array[LocatedFileStatus]()
            val filesIte = fileSystem.listFiles(folder.getPath, true)

            while (filesIte.hasNext) {
              filesArraySrc = filesArraySrc :+ filesIte.next()
            }

            val result: Boolean = filesArraySrc.filter(!_.isDirectory).map(
              file => {
                val destination = new Path(
                  dst.toString + file.getPath.toString.split(getPathWithDepth(file.getPath, getSrcWithoutWildCrd(src).depth()).toString)(1)
                )
                fileSystem.mkdirs(destination.getParent)
                fileSystem.rename(file.getPath, destination)
              }
            ).reduceOption(_ && _).getOrElse(false)

            filesArraySrc.foreach(f => fileSystem.delete(f.getPath, true))
            result
          }
        ).reduceOption(_ && _).getOrElse(false)
      }
    } match {
      case Failure(result) => throw new IOException(s"files move failed from: ${src.getName} to ${dst.getName}")
      case Success(result) => result
    }
  }

  /**
    * Move a file from one hdfs location to another (can be use for renaming ressources)
    *
    * @param src The source resource
    * @param dst The destination of the resource
    * @return true if the move is successful else false
    */
  def move(src: String, dst: String, prefix: Boolean, suffix: Boolean): Boolean = {
    val (hsrc, hdst) = (new Path(src), new Path(dst))
    flatMove(hsrc, hdst, prefix, suffix)
  }

  /**
    * Move a file / directory from one hdfs location to another and flatten hierarchy
    *
    * @param src    [[Path]] source
    * @param dest   [[Path]] destination
    * @param prefix prepend file modification time
    * @param suffix append timestamp
    * @return true if successful
    */
  def flatMove(src: Path, dest: Path, prefix: Boolean, suffix: Boolean): Boolean = {
    Try {
      val myFiles = getFilesFromPath(src.toString, recusiveness = true)

      val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

      fileSystem.mkdirs(dest)

      myFiles.forall(f => {
        val newFileName = (prefix, suffix) match {
          case (true, true) =>
            s"${sdf.format(f.getModificationTime)}_${f.getPath.getName}_${sdf.format(System.currentTimeMillis())}"
          case (true, false) =>
            s"${sdf.format(f.getModificationTime)}_${f.getPath.getName}"
          case (false, true) =>
            s"${f.getPath.getName}_${sdf.format(System.currentTimeMillis())}"
          case (false, false) => f.getPath.getName
        }

        fileSystem.rename(f.getPath, new Path(dest, new Path(newFileName)))
      })
    } match {
      case Failure(result) => throw new IOException(s"Files move failed from: ${src.getName} to ${dest.getName} , value of forall on moving files : $result")
      case Success(result) => result
    }
  }

  /**
    * Get LocatedFileStatus from a given resource path
    *
    * @param resourcePath the resource path
    * @param recusiveness if you want to go recurcive
    * @return a list of LocatedFileStatus
    */
  def getFilesFromPath(resourcePath: String, recusiveness: Boolean): List[LocatedFileStatus] = {
    var seqPath: List[LocatedFileStatus] = List()
    val lstFilesInit = fileSystem.globStatus(new Path(resourcePath))
    val lstFiles = lstFilesInit.map(f => fileSystem.listFiles(f.getPath, recusiveness))

    lstFiles.foreach(lst =>
      while (lst.hasNext) {
        val path = lst.next()
        if (!path.isDirectory)
          seqPath :+= path
      }
    )

    seqPath
  }

  /**
    * Move a file / directory from one hdfs location to another and flatten hierarchy
    * Wrapper on [[move(String, String, Boolean, Boolean)]]
    *
    * @param src    List of [[String]] sources
    * @param dst    [[String]] destination
    * @param prefix prepend file modification time
    * @param suffix append timestamp
    * @return true if successful
    */
  def flatMove(src: List[String], dst: String, prefix: Boolean, suffix: Boolean): Boolean = {
    src.forall(s => flatMove(s, dst, prefix, suffix))
  }

  /**
    * Move a file / directory from one hdfs location to another and flatten hierarchy
    *
    * @param src    [[String]] source
    * @param dst    [[String]] destination
    * @param prefix prepend file modification time
    * @param suffix append timestamp
    * @return true if successful
    */
  def flatMove(src: String, dst: String, prefix: Boolean, suffix: Boolean): Boolean = {
    val (hsrc, hdst) = (new Path(src), new Path(dst))
    flatMove(hsrc, hdst, prefix, suffix)
  }

  /**
    * Copy files from multiple HDFS path to another
    * Wrapper on [[copyPaste(String, String)]]
    *
    * @param src [[String]] list paths to file
    * @param dst [[String]] path to dest dir
    * @return true if successful, else false
    */
  def copyPaste(src: List[String], dst: String): Boolean = {
    src.forall(s => copyPaste(s, dst))
  }

  /**
    * Copy a file from one HDFS path to another
    * Wrapper on [[copyPaste(Path, Path)]]
    *
    * @param src [[String]] path to file
    * @param dst [[String]] path to dest dir
    * @return true if successful, else false
    */
  def copyPaste(src: String, dst: String): Boolean = {
    val (hsrc, hdst) = (new Path(src), new Path(dst))
    copyPaste(hsrc, hdst)
  }

  /**
    * Copy a file from one HDFS path to another
    *
    * @param src [[Path]] path to file
    * @param dst [[Path]] path to dest dir
    * @return true if successful, else false
    */
  def copyPaste(src: Path, dst: Path): Boolean = {
    if (!isGlob(src) && !fileSystem.exists(src)) {
      throw new UnsupportedOperationException("Source path does not exist: " + src.toString)
    } else {
      val filesListWithoutWildCd = fileSystem.globStatus(src)

      filesListWithoutWildCd.map(
        folder => {
          var filesArraySrc = Array[LocatedFileStatus]()
          val filesIte = fileSystem.listFiles(new Path(folder.getPath.toString), true)

          while (filesIte.hasNext) {
            filesArraySrc = filesArraySrc :+ filesIte.next()
          }

          val result: Boolean = filesArraySrc.filter(!_.isDirectory).map(
            file => {
              val destination = new Path(
                dst.toString + file.getPath.toString.split(getPathWithDepth(
                  file.getPath,
                  getSrcWithoutWildCrd(src).depth()
                ).toString)(1)
              )
              fileSystem.mkdirs(destination.getParent)
              FileUtil.copy(fileSystem, file.getPath, fileSystem, destination, false, true, fileSystem.getConf)
            }
          ).reduceOption(_ && _).getOrElse(false)
          result
        }
      ).reduceOption(_ && _).getOrElse(false)
    }
  }

  /**
    *
    *
    * @param src
    * @param dst
    * @param prefix
    * @param suffix
    * @return
    */
  def flatCopyPast(src: List[String], dst: String, prefix: Boolean, suffix: Boolean): Boolean = {
    src.forall(s => flatCopyPast(s, dst, prefix, suffix))
  }

  /**
    *
    * @param src
    * @param dst
    * @param prefix
    * @param suffix
    * @return
    */
  def flatCopyPast(src: String, dst: String, prefix: Boolean, suffix: Boolean): Boolean = {
    val (hsrc, hdst) = (new Path(src), new Path(dst))
    flatCopyPast(hsrc, hdst, prefix, suffix)
  }

  /**
    * Copy content of source and flatten hierarchy
    *
    * @param src    [[Path]] source
    * @param dest   [[Path]] destination
    * @param prefix [[Boolean]] prepend file modification time
    * @param suffix [[Boolean]] append current timestamp
    * @return
    */
  def flatCopyPast(src: Path, dest: Path, prefix: Boolean, suffix: Boolean): Boolean = {

    Try {
      val myFiles = getFilesFromPath(src.toString, recusiveness = true)
      val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")

      fileSystem.mkdirs(dest)

      myFiles.forall(f => {
        val newFileName = (prefix, suffix) match {
          case (true, true) =>
            s"${sdf.format(f.getModificationTime)}_${f.getPath.getName}_${sdf.format(System.currentTimeMillis())}"
          case (true, false) =>
            s"${sdf.format(f.getModificationTime)}_${f.getPath.getName}"
          case (false, true) =>
            s"${f.getPath.getName}_${sdf.format(System.currentTimeMillis())}"
          case (false, false) => f.getPath.getName
        }

        FileUtil.copy(fileSystem, f.getPath, fileSystem, new Path(dest, newFileName), false, true, fileSystem.getConf)
      })
    } match {
      case Failure(result) => throw new IOException(s"Files copy failed from: ${src.getName} to ${dest.getName}, value of forall on copying files : $result")
      case Success(result) => result
    }
  }

  def renameForce(src: Path, dst: Path): Boolean = {
    val dstParent = dst.getParent
    if (!fileSystem.exists(dstParent)) fileSystem.mkdirs(dstParent)
    fileSystem.rename(src, dst)
  }

  def isGlob(path: Path): Boolean = {
    val p = path.toString
    p.contains("*") || p.contains("$")
  }

  /**
    * ??
    *
    * @param path
    * @param depth
    * @return
    */
  def getPathWithDepth(path: Path, depth: Int): Path = {
    if (path.depth <= depth && fileSystem.isDirectory(path)) path
    else getPathWithDepth(path.getParent, depth)
  }

  /**
    * Remove wildcard from path
    *
    * @param src [[Path]]
    * @return [[Path]]
    */
  def getSrcWithoutWildCrd(src: Path): Path = {
    if (!src.toString.contains("*") && !src.toString.contains("?") && !src.toString.contains("[") && !src.toString.contains("]") && !src.toString.contains("\\")) src
    else getSrcWithoutWildCrd(src.getParent)
  }

  /**
    * List content from multiple paths
    *
    * @param paths The resourcePath you want to list on hdfs
    * @param loadOption   The way you want those resources to be sorted ie : each:last_modified_date:asc to sort resources by last modified date ascending.
    *                     last_modified_date and file_name are supported
    *                     asc and desc are supported
    * @param recursiveness
    * @return
    */
  def list(paths: List[String], loadOption: String, recursiveness: Boolean): List[String] = {
    val result = paths.flatMap(p => fileSystem
      .globStatus(new Path(p))
      .map(fileStatus => fileSystem.listFiles(new Path(fileStatus.getPath.toString), recursiveness))
      .flatMap(f => {
        var tmp: List[(String, Long)] = List()
        while (f.hasNext) {
          val path = f.next()
          if (!path.isDirectory) tmp :+= (path.getPath.toString, path.getModificationTime)
        }
        tmp
      })
    )

    sort(result, loadOption).map(_._1).toList
  }

  /**
    * Get content of file as String
    *
    * @param filePath
    * @return
    */
  def getFileAsString(filePath: String): String = {
    val stream = fileSystem.open(new Path(filePath))

    def lines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

    lines.takeWhile(_ != null).mkString("\n")
  }

  /**
    * Get List of all subdirectories of a folder
    *
    * @param dir
    * @return
    */
  def getListOfSubDir(dir: Path): Set[Object] = {
    val these =
      if (fileSystem.exists(dir)) fileSystem.listStatus(dir).filter(_.isDirectory).map(f => (f.getPath.getParent.getName, f.getPath)).toSet
      else Set()
    these ++ these.flatMap(x => getListOfSubDir(x._2))
  }

  /** Wrapper on [[getSequenceOfFilesNames(String, Option[String])]]
    *
    * @param readerPaths array of paths of a specific file or a folder on HDFS
    * @param loadOptions On which fields you want to sort (currently supported file name and last modification date)
    *                    ie : each:file_name:asc
    */
  def getSequenceOfFilesNames(readerPaths: List[String], loadOptions: Option[String]): Seq[String] = {
    var result: List[String] = List()
    readerPaths.foreach(path => result = result ++ getSequenceOfFilesNames(path, loadOptions))

    result
  }

  /** Get list of all files needed to build the dataframes, it returns either a sequence of one file name or
    * call the methods to get all files in specific folder sorted by loadOptions
    *
    * @param readerPath  path of a specific file or a folder on HDFS
    * @param loadOptions On which fields you want to sort (currently supported file name and last modification date)
    *                    ie : each:file_name:asc
    */
  def getSequenceOfFilesNames(readerPath: String, loadOptions: Option[String]): Seq[String] = {
    loadOptions.getOrElse("") match {
      case starts_with_each if starts_with_each.startsWith("each") =>
        list(readerPath, starts_with_each, true)
      //getListFilesSorted(readerPath.replaceAll("""\*""", ""), string_beginning_with_each)
      case _ => Seq(readerPath)
      //list(readerPath, "whatever", true)
    }
  }

  /**
    * List content of given path
    *
    * @param resourcePath The resourcePath you want to list on hdfs
    * @param loadOption   The way you want those resources to be sorted ie : each:last_modified_date:asc to sort resources by last modified date ascending.
    *                     last_modified_date and file_name are supported
    *                     asc and desc are supported
    * @param recursiveness
    * @return
    */
  def list(resourcePath: String, loadOption: String, recursiveness: Boolean): List[String] = {
    Try {
      val seqPath = getFilesFromPath(resourcePath, recursiveness)

      loadOption match {
        case "each:last_modified_date:asc" => seqPath.sortBy(_.getModificationTime).map(_.getPath.toString)
        case "each:last_modified_date:desc" => seqPath.sortBy(_.getModificationTime).reverse.map(_.getPath.toString)
        case "each:file_name:asc" => seqPath.sortBy(_.toString).map(_.getPath.toString)
        case "each:file_name:desc" => seqPath.sortBy(_.toString).reverse.map(_.getPath.toString)
        case _ => seqPath.map(_.getPath.toString)
      }
    } match {
      case Failure(result) => throw new FileNotFoundException(s"Path definition file not found on the classpath: $resourcePath")
      case Success(result) => result
    }
  }

  /** Get list of all files in the readerPath, sorting them by loadOption
    *
    * @param readerPath path of a specific file or a folder on HDFS
    * @param loadOption On which fields you want to sort (currently supported  file name and last modification date)
    *                   ie : each:file_name:asc
    * @return the sequence of files in specific folder sorted by loadOption
    */
  def getListFilesSorted(readerPath: String, loadOption: String): Seq[String] = {
    val lstFiles =
      if (!readerPath.contains("*"))
        fileSystem.listStatus(new Path(readerPath))
      else
        fileSystem.globStatus(new Path(
          readerPath.substring(0, readerPath.lastIndexOf("/")),
          readerPath.substring(readerPath.lastIndexOf("/") + 1)))

    val seqPath: Seq[(String, Long)] = lstFiles.filter(!_.isDirectory).map(
      f => (f.getPath.toString, f.getModificationTime)
    )

    sort(seqPath, loadOption).map(_._1)
  }

  /**
    * Sort content of a sequence of path & modification time
    *
    * @param paths [[Seq]] of ([[String]], [[Long]])
    * @param opt   sorting option
    * @return [[Seq]] of paths
    */
  private def sort(paths: Seq[(String, Long)], opt: String): Seq[(String, Long)] = opt match {
    case "each:last_modified_date:asc" => paths.sortBy(_._2)
    case "each:last_modified_date:desc" => paths.sortBy(_._2).reverse
    case "each:file_name:asc" => paths.sortBy(_._1)
    case "each:file_name:desc" => paths.sortBy(_._1).reverse
    case _ => paths
  }

  /**
    * This function uncompress a   file (*.gz,.zip,.tar,.Z)
    *
    * @param fsContext       : FileSystem
    * @param typeCompression :String
    * @param srcPath :String
    * @param dstPath  :String
    * @return Array[FileStatus]:String
    */
  def unCompressFiles(fsContext: FileSystem, typeCompression: String, srcPath: String, dstPath: String,archivePath :String): Array[FileStatus] = {
    val file = new Path(srcPath)
    val pathFolder: String = file.getParent.toString
    val fileShortName: String = file.getName
    val tempExtractionDir: String = typeCompression.toLowerCase().trim match {
      case "zip" => unZip(dstPath, srcPath,archivePath)
      case "gz" | "tar" | "z" | "tgz" => unPack(dstPath, srcPath,archivePath)
      case _ => throw new IllegalArgumentException("compress type not handle")
    }
    //archive file
    //  fsContext.rename(new Path(filePath), new Path(pathArchive + "/" + fileShortName + "_" + getDateAsString("yyyyMMddhhmmss", 0)))
    fsContext.listStatus(new Path(tempExtractionDir))
  }

  /**
    * This Function unzipped a  zip file
    *
    * @param dstPath      : String
    * @param srcPath :String
    * @param archivePath  :String
    * @return tempExtractionDir:String
    */
  private def unZip(dstPath: String, srcPath: String ,archivePath:String): String = {
    val tempExtractionDir = dstPath
    // zip file content
    getFilesFromPath(srcPath,true).map(_.getPath).map{ x =>
      val inFile = fileSystem.open(x)
      val in: BufferedInputStream = new BufferedInputStream(inFile)
      val zis: ZipInputStream = new ZipInputStream(in)

      // get the zipper file list entry
      var ze: ZipEntry = zis.getNextEntry

      while (ze != null) {
        val fileName = ze.getName
        if (!fileName.startsWith("__")) {
          if (!ze.isDirectory) {
            val outputFile = new FSDataOutputStream(fileSystem.create(new Path(tempExtractionDir + "/" + fileName)))
            var len: Int = zis.read(buffer)
            // Open file and write the content
            while (len > 0) {
              outputFile.write(buffer, 0, len)
              len = zis.read(buffer)
            }
            outputFile.close()

          } else if (ze.isDirectory) {
            fileSystem.mkdirs(new Path(tempExtractionDir + "/" + ze.getName))
          }
        }
        ze = zis.getNextEntry
      }
      zis.closeEntry()
      zis.close()

      flatMove(x,new Path(archivePath),false,true)
    }
    logger.info(">> tempExtractionDir : " + tempExtractionDir)

    tempExtractionDir
  }

  /**
    * This function uncompress a compress file (*.Z)
    *
    * @param srcPath      : String
    * @param archivePath :String
    * @param dstPath  :String
    * @return tempExtractionDir:String
    */
  private def unPack(dstPath: String, srcPath: String,archivePath:String): String = {
    //Fichier compresse
    //Dossier temporaire
    val tempExtractionDir = dstPath
    fileSystem.mkdirs(new Path(tempExtractionDir))
    //Process the files
    getFilesFromPath(srcPath,true).map(_.getPath).map {x =>
      val input = extract(uncompress(new BufferedInputStream(fileSystem.open(x))))
      createFile(input, srcPath, dstPath, fileSystem)
      flatMove(x,new Path(archivePath),false,true)
    }
    tempExtractionDir
  }

  /**
    * Utility function to extract content of an archive in input stream
    *
    * @param input [[InputStream]]
    * @return [[ArchiveInputStream]]
    */
  private def extract(input: InputStream): ArchiveInputStream = new ArchiveStreamFactory().createArchiveInputStream(input)

  /**
    * Utility function to uncompress an inputStream
    *
    * @param input [[BufferedInputStream]]
    * @return [[InputStream]]
    */
  private def uncompress(input: BufferedInputStream): InputStream = {
    Try(new CompressorStreamFactory().createCompressorInputStream(input)) match {
      case Success(i) => new BufferedInputStream(i)
      case Failure(_) => input
    }
  }

  /**
    * Utility function to write the content of an input stream to FS
    *
    * @param input     [[ArchiveInputStream]]
    * @param srcPath      Path to write to
    * @param dstPath Tmp folder
    * @param fsContext [[FileSystem]]
    */
  private def createFile(input: ArchiveInputStream, srcPath: String, dstPath: String, fsContext: FileSystem): Unit = {
    var entry = input.getNextEntry
    while (entry != null) {
      if (!entry.isDirectory) {
        val outputFileName = new FSDataOutputStream(fileSystem.create(new Path(dstPath + "/" + entry.getName)))
        val btoRead = new Array[Byte](1 * 1024)
        var len = input.read(btoRead)
        while (len != -1) {
          outputFileName.write(btoRead, 0, len)
          len = input.read(btoRead)
        }
        outputFileName.close()
      }
      else if(entry.isDirectory){
        fileSystem.mkdirs(new Path(dstPath + "/" + entry.getName))
      }
      entry = input.getNextEntry
    }

  }

  /**
    * SHOULD BE PRIVATE
    * get date
    */
  def getDateAsString(f: String, l: Long): String = {
    val calendar = Calendar.getInstance
    val today = if (l == 0) calendar.getTime else {
      calendar.setTimeInMillis(l)
      calendar.getTime
    }
    val dateFormat = new SimpleDateFormat(f)
    dateFormat.format(today)
  }

  /**
    * Merge src1 folder and src2 folder on dest folder
    *
    * @param src1
    * @param src2
    * @param dest
    * @return
    */
  def mergeFolder(src1: Path, src2: Path, dest: String,conf:Configuration) = {
    val listSubSrc1 = if (getListOfSubDir(src1).map(_.asInstanceOf[(String, Path)]).isEmpty) Seq()
    else getListSubFolder(getListOfSubDir(src1)
      .map(_.asInstanceOf[(String, Path)])
      .map(_._2.toString.stripPrefix(src1.toString))
      .toSeq)
    logger.info("------------------------source directory 1-----------="+listSubSrc1)
    val listSubSrc2 = if (getListOfSubDir(src2).map(_.asInstanceOf[(String, Path)]).isEmpty) Seq()
    else getListSubFolder(getListOfSubDir(src2)
      .map(_.asInstanceOf[(String, Path)])
      .map(_._2.toString.stripPrefix(src2.toString)).toSeq)
    logger.info("------------------------source directory 2----------=-"+listSubSrc2)
    if(!listSubSrc2.isEmpty) {
      val diff = listSubSrc1.diff(listSubSrc2)
      logger.info("Diff(scr1,scr1)="+diff)
      fileSystem.rename(src2, new Path(dest))
      diff.map{p =>val result=fileSystem.rename(new Path(src1.toString + "/" + p), new Path(dest+ "/" + p))
        if(!result){
          logger.info("retry copy for:"+p)
          FileUtil.copy(fileSystem, new Path(src1.toString + "/" + p),
            fileSystem,new Path(dest+ "/" + p),
            false,conf)
        }}

    }else fileSystem.rename(src1, new Path(dest))

  }

  /**
    * Utility ???
    * UNUSED
    *
    * @param a
    * @param b
    * @return
    */
  private def commonPrefix(a: String, b: String): String = {
    val common = (a zip b) takeWhile { t => t._1 == t._2 } map {
      _._1
    }
    common.mkString
  }

  /**
    *
    * @param s
    * @return
    */
  private def getListSubFolder(s: Seq[String]) = {
    val maxLength = s
      .reduceLeft((x, y) => if (x.split("/").length > y.split("/").length) x else y)
      .split("/")
      .length
    s.filter(_.split("/").length == maxLength)
  }

  /**
    * copy the files/folders from one path to another path in HDFS to back up the files
    *
    * @param srcPath
    * @param destPath
    * @param backupVersions
    */
  def backupFiles(srcPath : String,destPath : String,backupVersions : Int):Unit={
    val fileStatus = Try(fileSystem.listStatus(new Path(destPath))) match {
      case Success(f) => f
      case Failure(exception) => throw new Exception(exception)
    }
    if(fileStatus.length>=backupVersions)fileSystem.delete(fileStatus.sortBy(_.getModificationTime).head.getPath,true)
    fileSystem.rename(new Path(srcPath),new Path(destPath.concat("/").concat(new SimpleDateFormat("YYYYMMddHHmmss")
      .format(new Date()))))
  }
  def deleteEmptyDirectories(path : String): Unit = {
    fileSystem.listStatus(new Path(path)).filter(_.isDirectory).map(_.getPath.toString)
      .map { filePath => val fileSize = getFilesFromPath(filePath, true).size
        if (fileSize == 0) fileSystem.delete(new Path(filePath), true)
      }
  }
}
