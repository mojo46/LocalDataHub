/*
 * created by z024376
*/


package LocalDataHub


import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

object GenerateDataFrame {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    val utils = new Utils

    val schemaFile = args(0) //"..\\LocalDataHub\\output\\schema.xlsx"

    val dataFilePath_CSV = args(1) //"..\\LocalDataHub\\sampleResources\\core_dataset.csv"

    val outputFilePath_Excel = args(2) //"..\\LocalDataHub\\output\\core_dataset.xlsx"

    val dataFileDF = utils.readFromCsv(dataFilePath_CSV);
    println("datafile df=>");
    dataFileDF.show()

    val schemaDf = utils.readFromExcel(schemaFile);
    println("schema file df =>");
    schemaDf.show()

    val schema = generateSchema(schemaDf);
    println("schema => ");
    schema.printTreeString()

    val dataFrameWithSchema = createDataFrameWithSchema(dataFileDF, schema);
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