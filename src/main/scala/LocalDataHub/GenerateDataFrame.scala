/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

object GenerateDataFrame {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()


  def main(args: Array[String]): Unit = {

    val utils = new Utils

    val schemaFile = args(0) //"..\\LocalDataHub\\output\\schema.xlsx"

    val dataFilePath_CSV = args(1) //"D:\\LocalDataHub\\resources\\core_dataset.csv"

    val dataFileDF = utils.readFromCsv(dataFilePath_CSV)

    val schemaDf = utils.readFromExcel(schemaFile)

    val schema = generateSchema(schemaDf)

    val dataFrameWithSchema = createDataFrameWithSchema(dataFileDF, schema)

    dataFrameWithSchema.registerTempTable("coredata")

    spark.sql("select * from coredata")

    dataFrameWithSchema.show
  }

  //Generate schema from schemaDataFrame
  def generateSchema(schemaDF: DataFrame): StructType = {
    StructType(schemaDF.rdd.collect.toList
      .map(field =>
        StructField(field(0).toString, CatalystSqlParser.parseDataType(field(1).toString.replace("Type", "")), field(2).asInstanceOf[Boolean])))
  }

  def createDataFrameWithSchema(df: DataFrame, schema: StructType): DataFrame = {
    spark.createDataFrame(df.rdd, schema)
  }

  def createEmptyDF(schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row] /*spark.sparkContext.emptyRDD[Row]*/ , schema)
  }


}

