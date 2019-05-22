/*
 * created by z024376
*/

package LocalDataHub

import java.io.File
import java.nio.file.FileSystemNotFoundException

import org.apache.spark.{SparkContext, Success}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util._

object GenerateDataFrame {

  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    val utils = new Utils

    val schemaFile = args(0) //"..\\LocalDataHub\\output\\schema.xlsx"

    val dataFilePath = args(1) //"..\\LocalDataHub\\sampleResources\\core_dataset.csv"

    val outputFilePath_Excel = args(2) //"..\\LocalDataHub\\output\\core_dataset.xlsx"

    var dataDF: DataFrame = null

    val file = new File(dataFilePath)

    try {

      if (file.getName.endsWith("csv")) {

        dataDF = utils.readFromCsv(dataFilePath)
        println("---core data CSV FILE---")
        dataDF.show()
        dataDF.printSchema()

      }
      else if (file.getName.endsWith("xlsx")) {

        dataDF = utils.readFromExcel(dataFilePath)
        println("---core data EXCEL File---")
        dataDF.show()
        dataDF.printSchema()
      }
    }
    catch {
      case ex: Exception => {
        println("File shoul be either in CSV or Excel")
        sys.exit()
      }
    }

    val schemaDf = utils.readFromExcel(schemaFile);
    println("schema file df =>");
    schemaDf.show()

    val schema = generateSchema(schemaDf);
    println("schema => ");
    schema.printTreeString()

    if (dataDF.schema.length != schema.length) {
      println(s"The data File and the Schema File has Diffrent schema \n Data Contains :${dataDF.schema.length}\n Schema File Contains ${schema.length}")
      sys.exit()
    }
    if (dataDF.schema.fields != schema.fields) {
      println(s"The data File and Schema File have Diffrent schema")
      println(" Data File Schema :");
      dataDF.schema.printTreeString()
      println(" Schema in the Schema File:");
      schema.printTreeString()

      val zippedSchema = dataDF.schema.fields zip schema.fields

      val fieldsDiffType = zippedSchema.collect{
        case (a: StructField, b: StructField) if a.dataType != b.dataType =>
          (a.name, a.dataType)
      }
      fieldsDiffType.foreach(println)
      sys.exit
/*
      println("the following Fields names are diffrent from from the schema table kindly change the schema Names :")
*/


/*
      val fieldsDiffType = (sc zip (s2)).collect {
        case (a: StructField, b: StructField) if a.dataType != b.dataType => (a.name, a.dataType)
      }
      println("the following Fields Types are diffrent from from the schema table kindly change the schema Names :")
      fieldsDiffType.foreach(println)
*/
      sys.exit
    }

    /*
        try{
          dataDF.schema.length == schema.length
          dataDF.schema == schema
        }catch {
          case exception: Exception => {
            println("The data File and the Schema File has Diffrent schema")
          }
        }
    */


    val dataFrameWithSchema = createDataFrameWithSchema(dataDF, schema)
    println("dataframe with new schema=>");
    dataFrameWithSchema.show()

    utils.writeToExcel(outputFilePath_Excel, dataFrameWithSchema)

    utils.removeOtherFiles("..\\LocalDataHub\\output")
  }

  //Generate schema from schemaDataFrame
  def generateSchema(schemaDF: DataFrame): StructType = {
    schemaDF.rdd.collect.toList.foreach(println)
    StructType(schemaDF.rdd.collect.toList
      .map(field =>
        StructField(field(0).toString, CatalystSqlParser.parseDataType(field(1).toString.replace("Type", "")), field(2).toString.toBoolean)))
  }

  def createDataFrameWithSchema(df: DataFrame, schema: StructType): DataFrame = {
    spark.createDataFrame(df.rdd, schema)
  }

  def createEmptyDF(schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row] /*spark.sparkContext.emptyRDD[Row]*/ , schema)
  }

}