/*
 * created by z024376
*/

package LocalDataHub

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import LocalDataHub.{Utils,GenerateDataFrameFromSchemaFile,GenerateSchemaFile}


object test {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql
    println("mojo")

    val coreDataset = "..\\LocalDataHub\\resources\\core_dataset.csv"
    val schemaFileJson = "..\\LocalDataHub\\output\\schemaJson.json"
    val schemaFileCsv = "..\\LocalDataHub\\output\\schema.csv"
    val schemaFileExcel = "..\\LocalDataHub\\output\\schema.xlsx"
    val schemaFileText =  "..\\LocalDataHub\\output\\schema.txt"

    val utils = LocalDataHub.Utils
    val generateSchemaFile = LocalDataHub.GenerateSchemaFile
    val generateDataFrameFromSchemaFile = LocalDataHub.GenerateDataFrameFromSchemaFile


  //read the file into Dataframe
    val coreDF =utils.readFromCsv(coreDataset)
    println(s"code df ${coreDF.show}")
    coreDF.write.option("usehHader",value = true).mode(SaveMode.Overwrite).csv(schemaFileCsv)

    val csvdf = spark.read.option("useHeader",value = true).csv(schemaFileCsv)
    println("csv dataframe====")
    csvdf.show()

    sys.exit

    coreDF.printSchema()

/*
    //Create schema for new Table
    val schemas = StructType(Array(StructField("col_name",StringType,true),
      StructField("col_datatype",StringType,true),
      StructField("col_nullabletype",BooleanType,true)))
    schemas.printTreeString()
*/

    println("Create DataFrame form the schema of another table")
    //Create DataFrame form the schema of another table
    val schemaDf:DataFrame = generateSchemaFile.generateDataFrameFromSchema(coreDF)//coreDF.schema.map(m=>(m.name, m.dataType.toString,m.nullable)).toDF("name","type","nulla")
    schemaDf.show
    schemaDf.printSchema()

    //write the DataFrame to a excel file
     utils.writeToExcel(schemaFileExcel, schemaDf)

    //writeToCsv(schemaFileText,schemaDf)

    //sys.exit

    //read from a excel file
    val excelDF = utils.readFromExcel(schemaFileExcel);    println("read from excel file");    excelDF.show()
    //sys.exit

    //Creating schema from the structfileds --> schema DataFrame
    val schemaFromDFData = GenerateDataFrameFromSchemaFile.generateSchema(excelDF)//StructType(structFiled)

    //creating empty dataframe from Schema - (StructFiled)
    println("\n\nempty dataframe  from excel file ====> ")
    val emptyDF = generateDataFrameFromSchemaFile.createEmptyDF(schemaFromDFData)  //spark.createDataFrame(spark.sparkContext.emptyRDD[Row]/*spark.sparkContext.emptyRDD[Row]*/,structType1)
    emptyDF.show

    val newdf= generateDataFrameFromSchemaFile.createDataFrameWithSchema(coreDF,schemaFromDFData)
    println("DF with data")
    newdf.show

  }
}
