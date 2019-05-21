/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class Utils {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  //Function to Read from a excel file
  def readFromExcel(inputPath: String): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("useHeader", value = true)
      .option("treatEmptyValuesAsNulls",value = true)
      .option("timestampFormat",value = "dd-mm-yyyy HH:mm:ss")
      .load(inputPath)
  }

  //Function to Write to a excel file
  def writeToExcel(output: String, newdf: DataFrame): Unit = {
    /*val csvOutput = output+"schema.xlsx"*/
    newdf.coalesce(1).write
      .format("com.crealytics.spark.excel")
      .option("useHeader", value = true)
      .option("treatEmptyValuesAsNulls",value = true)
      .option("timestampFormat",value = "MM-dd-yyyy HH:mm:ss")
      .mode("overwrite")
      .save(output)

/*
    //Remove the .xlsx.crc file
    import java.io._
    val file = new File("..\\LocalDataHub\\output")
    val fileList = file.listFiles()

    fileList.foreach()
    if (file.exists()) file.delete()
    val file1 = new File("..\\LocalDataHub\\output\\.core_dataset.xlsx.crc")
    if (file1.exists()) file1.delete()
*/

  }

  //Function to Read from a CSV file
  def readFromCsv(inputPath: String): DataFrame = {
    spark.read
      .option("inferSchema", value = true)
      .option("header", value=true)
      .option("timestampFormat", "mm-dd-yyyy")
      .csv(inputPath)
  }

  //Function to Write into a Csv File
  def writeToCsv(outputPath: String, df: DataFrame): Unit = {
    df.coalesce(1).write
      .option("header", value = true)
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .option("treatEmptyValuesAsNulls", value = true)
      .mode("overwrite") // Optional, default: overwrite.
      .csv(outputPath)
  }

  //read as a dataframe from a orc file
  def readFromOrc(orcpath:String):DataFrame={
    spark.read
      .option("inferSchema", value = true)
      .option("useHeader",value = true)
      .orc(orcpath)
  }

  //write a dataframe to orc file
  def writeToOrc(orcpath:String,dataframe:DataFrame)={
    dataframe.write
      .option("useHeader",value = true)
      .mode(SaveMode.Overwrite)
      .save(orcpath)
  }

  //Remove the other files except xlsx and csv file in local file system

  def removeOtherFiles(path:String)={
    import java.io._
    val file = new File("..\\LocalDataHub\\output")
    val fileList = file.listFiles()
    fileList.foreach(f=>
      if(!(f.getName.endsWith("xlsx") || f.getName.endsWith("csv"))){
        f.delete()
      }
    )

  }

}
