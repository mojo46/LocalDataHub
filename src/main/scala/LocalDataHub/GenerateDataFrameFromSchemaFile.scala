/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

object GenerateDataFrameFromSchemaFile {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()
  import spark.implicits._

  //Generate schema from schemaDataFrame
  def generateSchema(schemaDF:DataFrame):StructType={
    StructType(schemaDF.rdd.collect.toList
      .map(field=>
        StructField(field(0).toString,CatalystSqlParser.parseDataType(field(1).toString.replace("Type", "")),field(2).asInstanceOf[Boolean])))
  }

  def createDataFrameWithSchema(df:DataFrame,schema:StructType): DataFrame ={
    spark.createDataFrame(df.rdd,schema)
  }

  def createEmptyDF(schema:StructType):DataFrame={
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row]/*spark.sparkContext.emptyRDD[Row]*/,schema)
  }



}
