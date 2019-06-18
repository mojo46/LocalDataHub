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

  val utils = new Utils

  def main(args: Array[String]): Unit = {

    val fileSystem = FileSystem.get(new Configuration())

    val utils = new Utils

    val inputDataFilePath = args(0) //"..\\LocalDataHub\\resources\\core_dataset.csv"

    val outputSchemaFilePath = args(1) //"..\\LocalDataHub\\schema\\schema.xlsx"

    val outputDataPath = args(2) //"D:\\LocalDataHub\\coreData"

    val dataDF: DataFrame = utils.readFileFromPath(inputDataFilePath)

    val schemaDF = generateDataFrameFromSchema(dataDF)
    println("---schema Df---")
    schemaDF.show()
    schemaDF.printSchema()

    println("--before write to excel file ---")
    val schemaPath = outputSchemaFilePath + "\\schema.xlsx"
    utils.writeToExcel(schemaPath, schemaDF)
    println("--after ---")
    println(outputSchemaFilePath)

    utils.removeOtherFiles(outputSchemaFilePath)

    val dataFileName: String = utils.findFileName(inputDataFilePath)

    def loadData(outputDataPath: String) = {

      val outputfile = outputDataPath + s"\\${dataFileName}"
      println(s"output data path where the data is stored ${outputfile}")
      utils.writeToCsv(outputfile, dataDF)
    }

    loadData(outputDataPath)
    utils.displayData(outputDataPath)
  }

  //Genanerating DataFrame From table MetaData
  def generateDataFrameFromSchema(coreDf: DataFrame): DataFrame = {
    coreDf.schema.map(m => (m.name, m.dataType, m.nullable.asInstanceOf[Boolean])).toDF("Column_Name", "DataType", "Nullable")
  }

}
