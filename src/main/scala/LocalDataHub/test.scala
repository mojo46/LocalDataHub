/*
 * created by z024376
*/

package LocalDataHub

import java.io.PrintWriter
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

object test {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql
    println("mojo")

    val coreDataset = "..\\LocalDataHub\\resources\\core_dataset.csv"
    val schemaFile = "..\\LocalDataHub\\output\\schemaJson.json"
    val excelFile = ""


  //read the file into Dataframe
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

    //Create schema for new Table
    import spark.implicits._
    val schemas = StructType(Array(StructField("col_name",StringType,true),
      StructField("col_datatype",StringType,true),
      StructField("col_nullabletype",BooleanType,true)))

    println("schemas ====>")
    schemas.printTreeString()

    println("\n\n\n")
/*

    val structfiled=schemas.map(m=>StructField(m.name,m.dataType,m.nullable)).toArray//.foreach(println)

    val structtype:StructType= StructType(structfiled)

    println("\n\n\n")
    println("Struct type")
    structtype.printTreeString()
*/
    //Create DataFrame form the schema of another table
    val schemaDf:DataFrame = coreDF.schema.map(m=>(m.name, m.dataType.toString,m.nullable)).toDF("name","type","nulla")
    println("data ====>")
    schemaDf.show
    schemaDf.printSchema()
    /*sys.exit()*/

    //preparing data for making it into StructField
    val map1= schemaDf.map(m=>{(m(0).toString, m(1).toString ,m(2).asInstanceOf[Boolean])}).rdd.collect
    map1.foreach(println)

    val structFiled = map1
      .map(m=>StructField(m._1, CatalystSqlParser.parseDataType(m._2.replace("Type", "")), m._3))

    //Creating schema from the structfileds --> schema DataFrame
    val structType1 = StructType(structFiled)

    //creating empty dataframe from Schema - (StructFiled)
    println("\n\nempty dataframe ====> ")
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row]/*spark.sparkContext.emptyRDD[Row]*/,structType1)

    df.show()

    /*
        structType1.printTreeString()
        val carstable = df.registerTempTable("cars")
        println("\n\nstruct type schema ====> ")
    */

/*
    val sampleSchema = StructType(Array(StructField("col1",StringType,true)))
    sampleSchema.printTreeString()
*/
    //val emptydf = spark.emptyDataFrame
//    sys.exit()


    /*

    def Datatype(s:String):DataType{

    val datatype:DataType = s

    }
    */

/*
    val data1 = Seq("name",1,true)
    val schemas = StructType(Array(StructField("col_name",StringType,true),
                                  StructField("col_datatype",StringType,true),
                                  StructField("col_nullabletype",BooleanType,true)))

    import spark.implicits._
    val se = Seq(Row("name","type","nulla"))
    val rdd = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    println("print RDD")
//    rdd.foreach(println)
rdd.show()
*/

/*
    val dfnew: DataFrame = rdd.toDF("name","type","nulla")
    print("dfnew =>>>>")
    dfnew.show(false)
*/
/*
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data).map(Row)//.toDF()
    rdd.show
    //val nedf = spark.read.json()

    val conf = new SparkConf;
   // val sc = new SparkContext
    //Created Empty RDD
    //var dataRDD = sc.emptyRDD[Row]

    val df =  spark.createDataFrame(dataRDD,data)
df.show()
*/

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
