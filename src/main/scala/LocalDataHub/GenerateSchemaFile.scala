/*
 * created by z024376
*/

package LocalDataHub

import java.io.File
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object GenerateSchemaFile {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val utils = new Utils

    val inputDataFilePath = args(0) //"..\\LocalDataHub\\resources\\core_dataset.csv"

    val outputSchemaFilePath = args(1) //"..\\LocalDataHub\\output\\schema.xlsx"

    var dataDF: DataFrame = null

    val file = new File(inputDataFilePath)

    try {

      if (file.getName.endsWith("csv")) {

        dataDF = utils.readFromCsv(inputDataFilePath)
        println("---core data CSV FILE---")
        dataDF.show()
        dataDF.printSchema()

      }
      else if (file.getName.endsWith("xlsx")) {

        dataDF = utils.readFromExcel(inputDataFilePath)
        println("---core data EXCEL File---")
        dataDF.show()
        dataDF.printSchema()
      }
    }
    catch {
      case ex: Exception => {
        println("File shoul be either in CSV or Excel")
      }
    }

    val schemaDF = generateDataFrameFromSchema(dataDF)
    println("---schema Df---")
    schemaDF.show()
    schemaDF.printSchema()

    println("--before write to excel file ---")
    utils.writeToExcel(outputSchemaFilePath, schemaDF)
    println("--after ---")

    utils.removeOtherFiles("..\\LocalDataHub\\output")

  }

  //Genanerating DataFrame From table MetaData
  def generateDataFrameFromSchema(coreDf: DataFrame): DataFrame = {
    coreDf.schema.map(m => (m.name, m.dataType.toString, m.nullable.asInstanceOf[Boolean])).toDF("Column_Name", "DataType", "Nullable")
  }

}
