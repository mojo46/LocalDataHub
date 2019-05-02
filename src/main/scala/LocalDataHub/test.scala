/*
 * created by z024376
*/

package LocalDataHub

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

object test {
  val spark = SparkSession.builder().appName("barcode").master("local").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql
    println("mojo")

    val coreDataset = "..\\LocalDataHub\\resources\\core_dataset.csv"
    val schemaFileJson = "..\\LocalDataHub\\output\\schemaJson.json"
    val schemaFileCsv = "..\\LocalDataHub\\output\\schema.csv"
    val schemaFileExcel = "..\\LocalDataHub\\output\\schema.xlsx"
    val schemaFileText =  "..\\LocalDataHub\\output\\schema.txt"




  //read the file into Dataframe
    val coreDF =readFromCsv(coreDataset)

    coreDF.printSchema()
    //coreDF.show()
    /*
    val newdfschema= StructType(StructField(Employee Name,StringType,true), StructField(Employee Number,IntegerType,true), StructField(State,StringType,true))
    println(newdfschema)
*/
    //coreDF.registerTempTable("hivetable")
   // coreDF.schema.prettyJson.foreach(println)


/*
    //Create schema for new Table
    val schemas = StructType(Array(StructField("col_name",StringType,true),
      StructField("col_datatype",StringType,true),
      StructField("col_nullabletype",BooleanType,true)))
    println("schemas ====>")
    schemas.printTreeString()
*/

    println("Create DataFrame form the schema of another table")
    //Create DataFrame form the schema of another table
    val schemaDf:DataFrame = generateDataFrameFromSchema(coreDF)//coreDF.schema.map(m=>(m.name, m.dataType.toString,m.nullable)).toDF("name","type","nulla")
    schemaDf.show
    schemaDf.printSchema()

    //write the DataFrame to a excel file
     //writeToExcel(schemaFileExcel, schemaDf)

    //writeToCsv(schemaFileText,schemaDf)

    //sys.exit

    //read from a excel file
    val excelDF = readFromExcel(schemaFileExcel);    println("read from excel file");    excelDF.show()
    //sys.exit

    //Creating schema from the structfileds --> schema DataFrame
    val schemaFromDFData = generateSchema(excelDF)//StructType(structFiled)

    //creating empty dataframe from Schema - (StructFiled)
    println("\n\nempty dataframe  from excel file ====> ")
    val emptyDF = createEmptyDF(schemaFromDFData)  //spark.createDataFrame(spark.sparkContext.emptyRDD[Row]/*spark.sparkContext.emptyRDD[Row]*/,structType1)
    emptyDF.show

    val newdf= spark.createDataFrame(coreDF.rdd,schemaFromDFData)
    println("DF with data")
    newdf.show

  }

  def generateDataFrameFromSchema(coreDf:DataFrame):DataFrame={
    coreDf.schema.map(m=>(m.name, m.dataType.toString,m.nullable)).toDF("Column_Name","DataType","Nullable")
  }

  //Generate schema from schemaDataFrame
  def generateSchema(schemaDF:DataFrame):StructType={
    StructType(schemaDF.rdd.collect.toList
      .map(field=>
        StructField(field(0).toString,CatalystSqlParser.parseDataType(field(1).toString.replace("Type", "")),field(2).asInstanceOf[Boolean])))
  }


  def createEmptyDF(schema:StructType):DataFrame={
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row]/*spark.sparkContext.emptyRDD[Row]*/,schema)
  }

  //Function to Read from a excel file
  def readFromExcel(inputPath: String): DataFrame = {
    spark.read.format("com.crealytics.spark.excel")
      .option("useHeader", value=true)
      .option("treatEmptyValuesAsNulls",value = true)
      .option("inferSchema",value = true)
      //.option("addColorColumns",value = true)
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .load(inputPath)
  }

  //Function to Write to a excel file
  // function to write to a excel file
  def writeToExcel(output: String, newdf: DataFrame): Unit = {
    newdf.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", true)
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
      .mode("overwrite") // Optional, default: overwrite.
      .save(output)

    import java.io._
    val file = new File("D:\\LocalDataHub\\output\\.schema.xlsx.crc")
    if(file.exists())file.delete()

  }

  //Function to Read from a CSV file
  def readFromCsv(inputPath:String):DataFrame ={
    spark.read
      .option("inferSchema",value = true)
      .option("header",value = true)
      .option("treatEmptyValuesAsNulls",value = true)
      .option("inferSchema",value = true)
      //.option("addColorColumns",value = true)
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      .csv(inputPath)
  }

  //Function to Write into a Csv File
  def writeToCsv(outputPath:String,df:DataFrame):Unit ={
    //df.write.option("overwrite",value = true).text(outputPath)
  }

  /*
      val structfiled=schemas.map(m=>StructField(m.name,m.dataType,m.nullable)).toArray//.foreach(println)
      val structtype:StructType= StructType(structfiled)
      println("\n\n\n")
      println("Struct type")
      structtype.printTreeString()
  */

  /*//write the dataframe into a file
writeToCsv(schemaFileCsv,schemaDf)

//read the schema file
val schemaCsvDF = readFromCsv(schemaFileCsv)
schemaCsvDF.show()*/

  /*sys.exit()*/

  //preparing data for making it into StructField
  /*
  val tstdf:DataFrame= null
      val structType1= generatschema(schemaDf)
  */

  /*
      val map1= schemaDf.map(m=>{(m(0).toString, m(1).toString ,m(2).asInstanceOf[Boolean])}).rdd.collect
      map1.foreach(println)
      val structFiled = map1
        .map(m=>StructField(m._1, CatalystSqlParser.parseDataType(m._2.replace("Type", "")), m._3))
  */


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
