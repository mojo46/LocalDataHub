package com.renault.datalake.dll.common.core.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StructType}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
  * Tools
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
object Tools {


  val BUFFER: Int = 2048
  val buffer = new Array[Byte](1024)
  private val primitiveTypes = Seq(
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    ByteType,
    ShortType,
    DateType,
    TimestampType,
    BinaryType,
    CalendarIntervalType,
    NullType
  )

  /**
    * prepare job config
    *
    * @param str   [[String]] ??
    * @param param [[String]] ??
    * @return [[String]] Enriched config
    */
  def prepareString(str: String, param: String): String = {
    var r = str
    val pattern = new Regex("\\{([a-zA-Z0-9\\_\\-])+\\}")

    pattern.findAllIn(str).toArray.foreach { f => r = r.replaceAllLiterally(f, param) }

    r
  }

  /**
    * Chain calls to [[DataFrame]] cast, compute & process
    *
    * @param df               [[DataFrame]] to apply logic to
    * @param fieldsMap        [[Seq]] of (name, alias, field type) used to cast dataframe columns
    * @param computeFieldsMap [[Seq]] of (name, field type, SQL command) to apply to DF
    * @param filter           SQL filter instruction
    * @return processed [[DataFrame]]
    */
  def castComputeAndProcess(
                             df: DataFrame,
                             fieldsMap: Seq[(String, String, String)],
                             computeFieldsMap: Seq[(String, String, String,Option[Int])],
                             filter: String
                           ): DataFrame = {
    processDataframe(
      computeDataframe(
        castDataframe(df, fieldsMap),
        computeFieldsMap
      ),
      filter
    )
  }

  /**
    * Cast fields of given [[DataFrame]] according to given input
    *
    * @param df        [[DataFrame]] to cast
    * @param fieldsMap [[Seq]] of (name, alias, field type) used to cast dataframe columns
    * @return [[DataFrame]] of casted column
    */
  def castDataframe(df: DataFrame, fieldsMap: Seq[(String, String, String)]): DataFrame = {
    val castFields = fieldsMap.map {
      case (name, alias, fieldtype) => s"cast($name as ${evalDataTypes(fieldtype)}) as ${if (alias.isEmpty) name else alias}"
    }
    df.selectExpr(castFields: _*)
  }

  /**
    * Apply computation to a [[DataFrame]]
    *
    * @param df               [[DataFrame]] to compute
    * @param computeFieldsMap [[Seq]] of (name, field type, SQL command) to apply to DF
    * @return computed [[DataFrame]]
    */
  def computeDataframe(df: DataFrame, computeFieldsMap: Seq[(String, String, String,Option[Int])]): DataFrame = {

    val computeFieldsMapNonNull: Seq[(String, String, String,Option[Int])] = Option(computeFieldsMap).getOrElse(Seq())
    val fields = df.columns.mkString(",")
    val technicalFields = Seq(
      ("DLL_VALIDITE", "Int", "0",None),
      ("DLL_CHECKSUM", "Int", s"hash($fields)",None),
      ("DLL_DATE_MAJ", "timestamp", "from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')",None),
      ("DLL_DATE_CREATION", "timestamp", "from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')",None)
    ).filter(e => computeFieldsMapNonNull.count(a => a._1 == e._1) == 0 && df.columns.count(_ equalsIgnoreCase e._1) == 0)

    val computeFieldsWithTechnicalFields = computeFieldsMapNonNull.filter(m=>(!m._4.isDefined)||(m._4==null)) ++ technicalFields

    val outputDf = computeFieldsWithTechnicalFields match {
      case null => df
      case _ =>
        val castComputeFields = computeFieldsWithTechnicalFields.map {
          case (name, fieldtype, sqlCommand,sort) => s"cast($sqlCommand as ${evalDataTypes(fieldtype)}) $name"
        }.+:("*")
        df.selectExpr(castComputeFields: _*)
    }
    val extracFields=computeFieldsMapNonNull.filter(m=>(m._4.isDefined)&&(m._4.get>0))
      .sortBy(_._4.get)

    extracFields.size match {
      case 0=> outputDf
      case _ =>extracFields.foldLeft(outputDf)((memoDF, computedField) =>
        memoDF.withColumn(computedField._1,expr("cast(" + computedField._3 + " as " + Tools.evalDataTypes(computedField._2) + ") ")))
    }
  }

  /**
    * For a given SQL data type, get it's HQL equivalent
    *
    * @param fieldType SQL data type
    */
  def evalDataTypes(fieldType: String): String = {
    val evalDecimal = """(?i)(decimal.*)""".r
    val evalInterval = """(?i)(interval.*)""".r
    val evalComplexType = "(?i)(STRUCT.*|ARRAY.*|MAP.*|UNIONTYPE.*)".r

    fieldType.replaceAll("\\s", "") match {
      case evalComplexType(s) => s.toLowerCase()
      case ci"integer" => "int"
      case ci"Int" => "int"
      case ci"TINYINT" => "TINYINT"
      case ci"SMALLINT" => "SMALLINT"
      case ci"BIGINT" => "BIGINT"
      case ci"NUMERIC" => "NUMERIC"
      case ci"BINARY" => "BINARY"
      case ci"VARCHAR" => "VARCHAR"
      case evalInterval(s) => s.toLowerCase()
      case ci"DATE" => "date"
      case ci"timestamp" => "timestamp"
      case ci"float" => "float"
      case ci"double" => "double"
      case evalDecimal(s) => s.toLowerCase
      case ci"bigint" => "bigint"
      case ci"boolean" => "boolean"
      case ci"String" => "string"
      case ci"NULL" => "NULL"
      case _ => throw new Exception(fieldType + "is unsupported type by DLL")
    }
  }

  /**
    * Convert string to case insensitive regex for matching
    *
    * @param sc [[StringContext]] to convert
    */
  implicit class CaseInsensitiveRegex(sc: StringContext) {
    def ci: Regex = ("(?i)" + sc.parts.mkString).r
  }

  /**
    * Apply filter to given [[DataFrame]]
    *
    * @param df     [[DataFrame]] to filter
    * @param filter SQL filter instruction
    * @return filtered [[DataFrame]]
    */
  def processDataframe(df: DataFrame, filter: String): DataFrame = {
    filter match {
      case null => df
      case _ => df.filter(filter)
    }
  }

  /**
    * Delete given files
    *
    * @param fsContext       FS handler
    * @param compressedFiles list of paths to delete
    */
  def deleteFolderTmp(fsContext: FileSystem, compressedFiles: Array[String]): Unit = {
    compressedFiles.foreach { f => fsContext.delete(new Path(f), true) }
  }

  /**
    * Convert given [[DateType]] to Hive readable [[String]]
    *
    * @param t [[DateType]] to convert
    * @return converted [[String]]
    */
  def toHIveString(t: DataType): String = t match {
    case (t) if primitiveTypes contains t => t.simpleString
    case t: DecimalType => createDecimalType(t.precision, t.scale).simpleString
    case t: MapType => t.simpleString
    case t: ArrayType => t.simpleString
    case t: StructType => t.simpleString
    case other: DataType => throw new UnsupportedOperationException(s"Unexpected data type: $other")
  }

  /**
    * Convert case class object to map (fields -> values)
    *
    * @param cc : case class Object
    * @return [[Map]] of [[String]] -> [[Any]]
    */
  def getCaseClassParamsAndValues(cc: AnyRef): Map[String, Any] =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }.filter(!_._1.contains("$"))

  /**
    * Get fileName, file Size, and file modification date of file
    *
    * @param filePath : path to file
    */
  def getFileInfo(fsContext: FileSystem, filePath: String, fileChecksum: String): FileInfo = {

    val fileStatus = Try(fsContext.listStatus(new Path(filePath)).head) match {
      case Success(f) => f
      case Failure(exception) => throw new Exception(exception)
    }

    FileInfo(
      fileStatus.getPath.getName,
      fileStatus.getLen,
      getDateAsString("yyyy-MM-dd'T'HH:mm:ss", fileStatus.getModificationTime),
      fileChecksum
    )
  }

  /**
    * Format timestamp in milliseconds
    *
    * @param f Format to apply
    * @param l timestamp
    * @return Formatted timestamp
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



  case class FileInfo(DLL_FILE_NAME: String, DLL_FILE_SIZE: Long, DLL_FILE_LAST_MODIFIED: String, DLL_FILE_CHECKSUM: String)


  /**
    *
    * @param dataFrames
    * @param fieldNames
    * @param orderBy
    */
  def dropDuplicate(dataFrames:Seq[DataFrame],fieldNames:Array[String],orderBy:Option[(String,String)]):Seq[DataFrame]={
    import org.apache.spark.sql.functions.col
    dataFrames.map(dataFrame=>{
      fieldNames.isEmpty match {
        case true => dataFrame.dropDuplicates()
        case false=>{
          if((orderBy.isDefined)&&(orderBy.get!=("","")))
          {
            dataFrame.orderBy(if(orderBy.get._2.equalsIgnoreCase("asc"))
              col(orderBy.get._1).asc else col(orderBy.get._1).desc).dropDuplicates(fieldNames)
          }else dataFrame.dropDuplicates(fieldNames)
        }
      }
    })

  }








}
