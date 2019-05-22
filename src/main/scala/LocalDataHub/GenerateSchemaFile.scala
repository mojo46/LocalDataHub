/*
 * created by z024376
*/

package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object GenerateSchemaFile {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {


    //val fileSystem = FileSystem.get(new Configuration())

    val utils = new Utils

    val csvDataFilePath = args(0) //"..\\LocalDataHub\\resources\\core_dataset.csv"

    val outputExcelPath = args(1) //"..\\LocalDataHub\\output\\schema.xlsx"

    val csvDataDF = utils.readFromCsv(csvDataFilePath)
    println("---core data---")
    csvDataDF.show()
    csvDataDF.printSchema()

    val schemaDF = generateDataFrameFromSchema(csvDataDF)
    println("---schema Df---")
    schemaDF.show()
    schemaDF.printSchema()

/*
    println("---before write to csv file---")
    utils.writeToCsv(outputCsvPath, schemaDF)
    println("--after ---")
*/

    println("--before write to excel file ---")
    utils.writeToExcel(outputExcelPath, schemaDF)
    println("--after ---")

/*
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
*/

/*
    val list =getFilesFromPath("D:\\LocalDataHub\\output",true)
    list.map(m=>m.getPath).foreach(println)

     def merge(srcPath: String, dstPath: String): Unit = {
       val hadoopConfig = new Configuration()
       val hdfs = FileSystem.get(hadoopConfig)
       FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
       // the "true" setting deletes the source files once they are merged into the new output
     }

      merge(outputExcelPath,s"D:\\LocalDataHub\\output\\data.csv")
*/

    utils.removeOtherFiles("..\\LocalDataHub\\output")

  }

  //Genanerating DataFrame From table MetaData
  def generateDataFrameFromSchema(coreDf: DataFrame): DataFrame = {
    coreDf.schema.map(m => (m.name, m.dataType.toString, m.nullable.asInstanceOf[Boolean])).toDF("Column_Name", "DataType", "Nullable")
  }

}
