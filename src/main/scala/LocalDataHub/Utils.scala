/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, SparkSession}

class Utils {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  //Function to Read from a excel file
  def readFromExcel(inputPath: String): DataFrame = {
    spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", value = true)
      .option("treatEmptyValuesAsNulls", value = true)
      .option("inferSchema", value = true)
      //.option("addColorColumns",value = true)
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .load(inputPath)
  }

  //Function to Write to a excel file
  def writeToExcel(output: String, newdf: DataFrame): Unit = {
    newdf.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", value = true)
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("overwrite") // Optional, default: overwrite.
      .save(output)

    //Remove the .xlsx.crc file
    import java.io._
    val file = new File("..\\LocalDataHub\\output\\.schema.xlsx.crc")
    if (file.exists()) file.delete()
  }

  //Function to Read from a CSV file
  def readFromCsv(inputPath: String): DataFrame = {
    spark.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .option("treatEmptyValuesAsNulls", value = true)
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .csv(inputPath)
  }

  //Function to Write into a Csv File
  def writeToCsv(outputPath: String, df: DataFrame): Unit = {
    df.write
      .option("header", value = true)
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .option("treatEmptyValuesAsNulls", value = true)
      .mode("overwrite") // Optional, default: overwrite.
      .csv(outputPath)
  }

}
