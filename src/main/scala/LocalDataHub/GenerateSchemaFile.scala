/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GenerateSchemaFile {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  import spark.implicits._


  def main(args: Array[String]): Unit = {

    val utils = new Utils

    val csvFilePath = args(0)

    val outputExcelPath = args(1)

    val csvDF = utils.readFromCsv(csvFilePath)
    println(s"Data file ${csvDF.show}")

    val schemaDF = generateDataFrameFromSchema(csvDF)
    println(s"Data file ${schemaDF.show}")

    utils.writeToExcel(outputExcelPath, schemaDF)

  }

  //Genanerating DataFrame From table MetaData
  def generateDataFrameFromSchema(coreDf: DataFrame): DataFrame = {
    coreDf.schema.map(m => (m.name, m.dataType.toString, m.nullable)).toDF("Column_Name", "DataType", "Nullable")
  }


}
