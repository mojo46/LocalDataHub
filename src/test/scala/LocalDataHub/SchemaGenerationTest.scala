package LocalDataHub

import org.apache.spark._
import org.scalatest._
import org.apache.spark.sql.{SparkSession, DataFrame, Row}

class SchemaGenerationTest extends FunSuite {
  val spark = SparkSession.builder().appName("schema generation").master("local").getOrCreate()

  val schemaFile = "..\\LocalDataHub\\output\\schema.xlsx"

  val dataFilePath_CSV = "D:\\LocalDataHub\\resources\\core_dataset.csv"

  val

  test("Schema generation") {

    val utils = new Utils

    val schemaDF = utils.readFromExcel(schemaFile)
    assert(schemaDF.show() != null)
    val schema = GenerateDataFrame.generateSchema(schemaDF)
    assert(schema.printTreeString() != null)
    val newDF = GenerateDataFrame.createDataFrameWithSchema(utils.readFromCsv(dataFilePath_CSV), schema)
    newDF.write.format("orc").save()
    assert(newDF.show() != null)


  }

}
