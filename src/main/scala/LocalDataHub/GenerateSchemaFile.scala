/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser


object GenerateSchemaFile {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()
  import spark.implicits._



}
