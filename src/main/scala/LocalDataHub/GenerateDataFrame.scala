/*
 * created by z024376
*/

package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

object GenerateDataFrame {

  val spark = SparkSession.builder().appName("barcode").enableHiveSupport().master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    val utils = new Utils

    val schemaFile = args(0) //"D:\\LocalDataHub\\schema\\schema.xlsx"

    val dataFilePath = args(1) //"D:\\LocalDataHub\\resources\\core_dataset_Feb.csv"

    val outputFilePath = args(2) //"D:\\LocalDataHub\\coreData"

    val dataDF: DataFrame = utils.readFileFromPath(dataFilePath)

    val dataFileName: String = utils.findFileName(dataFilePath)

    val schemaDf = utils.readFromExcel(schemaFile)

    println("schema file df =>")
    schemaDf.show()

    val schema = generateSchema(schemaDf)
    println("schema Generated from schema File ===> ")
    schema.printTreeString()

    schemaValidation(dataDF.schema, schema)

    val dataFrameWithSchema = createDataFrameWithSchema(dataDF, schema)
    println("dataframe with new schema=>")
    dataFrameWithSchema.show()
    dataFrameWithSchema.count()

    val outputfile = outputFilePath + s"\\${dataFileName}"

    println(s"output data path where the data is stored ${outputfile}")
    utils.writeToCsv(outputfile, dataFrameWithSchema)

    val dataPath = outputFilePath + "\\*.csv"
    val coreData = utils.readFromCsv(dataPath: String)
    coreData.show(truncate = true)
    println(s"Number Of Recodes in the table :${coreData.count()}")

  }

  // Validate schema
  def schemaValidation(inputFileSchema: StructType, schemaFileSchema: StructType): Unit = {

    if (inputFileSchema.length != schemaFileSchema.length) {
      println(s"The data File and the Schema File has Diffrent schema \n Data Contains :${inputFileSchema.length}\n Schema File Contains ${schemaFileSchema.length}")
      sys.exit()
    }
    else {
      println("There is no issue with column length\n")
    }

    println("\nChecking for Columns with Difference in Names or DataType or Order\n")

    if (inputFileSchema != schemaFileSchema) {

      println("\nThere is some Column miss match between ==> Columns in the Schema File and Columns of the Data File <===\n=====> Check with the Schema File")

      val differenceInNames = (inputFileSchema.fields zip schemaFileSchema.fields).collect {
        case (a: StructField, b: StructField) if (a.name != b.name) && (a.dataType != b.dataType) => (a.name, a.dataType)
      }
      if (differenceInNames.length != 0) {
        println(s"\nnumber of Columns with different Names or DataType or Order: ${differenceInNames.length}")
        println("\nColumns with different Names or DataType or Order\n")
        differenceInNames.foreach(println)
      }
      sys.exit()
    } else println("There is no issue with the schema")

  }

  //Generate schema from schemaDataFrame
  def generateSchema(schemaDF: DataFrame): StructType = {
    StructType(schemaDF.rdd.collect.toList
      .map(field =>
        StructField(field(0).toString, CatalystSqlParser.parseDataType(field(1).toString.replace("Type", "")), field(2).toString.toBoolean)))
  }

  //Create a DataFrame with a schema
  def createDataFrameWithSchema(df: DataFrame, schema: StructType): DataFrame = {
    spark.createDataFrame(df.rdd, schema)
  }

  //Create a empty DataFrame with a schema
  def createEmptyDF(schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row] /*spark.sparkContext.emptyRDD[Row]*/ , schema)
  }

}