/*
 * created by z024376
*/


package LocalDataHub

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import LocalDataHub.Utils

object GenerateSchemaFile {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()
  import spark.implicits._

  //Genanerating DataFrame From table MetaData
  def generateDataFrameFromSchema(coreDf:DataFrame):DataFrame={
    coreDf.schema.map(m=>(m.name, m.dataType.toString,m.nullable)).toDF("Column_Name","DataType","Nullable")
  }


}
