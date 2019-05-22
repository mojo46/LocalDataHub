/*
 * created by z024376
*/

package LocalDataHub

import java.io.File
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}


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

      }
      else if (file.getName.endsWith("xlsx")) {

        dataDF = utils.readFromExcel(dataFilePath)

      }
    }
    catch {
      case ex: Exception => {
        println("File should be either a CSV File or a Excel File")
        sys.exit()
      }
    }

    val schemaDf = utils.readFromExcel(schemaFile);
    println("schema file df =>");
    schemaDf.show()

    val schema = generateSchema(schemaDf);
    println("schema Generated from schema File=> ");
    schema.printTreeString()

    println("Comparing the number of cloumns ")
    if (dataDF.schema.length != schema.length) {
      println(s"The data File and the Schema File has Diffrent schema \n Data Contains :${dataDF.schema.length}\n Schema File Contains ${schema.length}")
      sys.exit()
    }
    else {
      println("There is no issue with column length")
    }

    println("\nChecking for Columns with Diffrent names")

    if(dataDF.schema != schema) {

      println("\nThere is some Column miss match =====> Check with the Schema File")

      val diffrenceInNames = (dataDF.schema.fields zip schema.fields).collect{
        case (a:StructField,b:StructField) if (a.name != b.name)&&(a.dataType != b.dataType) => (a.name,a.dataType)
      }
      if(diffrenceInNames.length != 0){
        println(s"\nnumber of Columns diffrent Names or DataType or Order: ${diffrenceInNames.length}")
        println("\nColumns diffrent Names or DataType or Order\n\n")
        diffrenceInNames.foreach(println)
      }

      sys.exit()

    }

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