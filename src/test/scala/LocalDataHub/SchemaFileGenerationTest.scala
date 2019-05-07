/*
 * created by z024376
*/


package LocalDataHub

import org.scalatest._
import org.apache.spark.sql.SparkSession
import java.io.File

class SchemaFileGenerationTest extends FunSuite {

  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    test("schema file generation") {

      val inputCSVFilePath = "D:\\LocalDataHub\\resources\\core_dataset.csv"

      val outputExcelPath = "D:\\LocalDataHub\\output\\schema.xlsx"

      val args: Array[String] = Array(inputCSVFilePath, outputExcelPath)

      GenerateSchemaFile.main(args)

      val file = new File(outputExcelPath)

      assert(file.exists(),"file exits")

    }

  }

}
