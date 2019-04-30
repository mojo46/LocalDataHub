/*
 * created by z024376
*/


package LocalDataHub

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object test {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()


  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql
    println("mojo")

    val coreDataset = "..\\LocalDataHub\\resources\\core_dataset.csv"
    val schemaFile = "..\\LocalDataHub\\output\\schemaJson.json"

    val coreDF = spark.read
      .option("inferSchema",value = true)
      .option("header",value = true)
      .option("timestampFormat", "dd-mm-yyyy")
      .csv(coreDataset)

    coreDF.printSchema()
    //coreDF.show()
    /*
    val newdfschema= StructType(StructField(Employee Name,StringType,true), StructField(Employee Number,IntegerType,true), StructField(State,StringType,true))
    println(newdfschema)
*/
    //coreDF.registerTempTable("hivetable")
   // coreDF.schema.prettyJson.foreach(println)

    val data: Seq[(String, DataType, Boolean)] = coreDF.schema.map(m=>(m.name,m.dataType,m.nullable))
    println("data ====>")
    data.foreach(println)

    val data1 = Seq("name",1,true)
    val schema = StructType(Array(StructField("col_name",StringType,true),
                                  StructField("col_datatype",StringType,true),
                                  StructField("col_nullabletype",StringType,true)))

    import spark.implicits._
val se = Seq("name","type","nulla")
    val rdd = spark.sparkContext.parallelize(data)
    println("print RDD")
    rdd.foreach(println)

    val dfnew: DataFrame = rdd.toDF()  //["name","type","nulla"]
    print("dfnew =>>>>")
    dfnew.show(false)

    /*
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data).map(Row)//.toDF()
    rdd.show*/
    //val nedf = spark.read.json()

/*    val conf = new SparkConf;*/
   // val sc = new SparkContext
    //Created Empty RDD
    //var dataRDD = sc.emptyRDD[Row]

    /*val df =  spark.createDataFrame(dataRDD,data)
df.show()*/

/*
    //data.foreach(println)
    import java.io._
    val a = /*coreDF.schema.toList*/coreDF.schema.json
    val writer = new PrintWriter(new File(schemaFile))
    writer.write(a)
    writer.close()

    val df = spark.read
      .option("inferSchema",value = true)
      .option("header",value = true)
      .option("timestampFormat", "dd-mm-yyyy")
      .json(schemaFile)
    df.show()
*/

/*    new PrintWriter(schemaFile) { write(coreDF.schema.); close }*/

  }

/*

  // function to read from a excel file
  def readFromExcel(inputPath: String): DataFrame = {

    spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", value=true)
      .option("treatEmptyValuesAsNulls",value = true)
      .option("inferSchema",value = true)
      /*.option("addColorColumns",value = true)*/
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .load(inputPath)

  }

  // function to write to a excel file
  def writeToExcel(output: String, newdf: DataFrame): Unit = {
    newdf.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", value = true)
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("overwrite") // Optional, default: overwrite.
      .save(output)
  }

*/


}
