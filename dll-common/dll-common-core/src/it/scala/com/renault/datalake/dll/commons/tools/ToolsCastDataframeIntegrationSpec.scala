package com.renault.datalake.dll.commons.tools

import com.renault.datalake.dll.common.core.util.Tools
import com.renault.datalake.dll.common.test.SparkSqlSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{FeatureSpec, Matchers}


/**
  * Employee
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
protected case class Employee(matricule: String,
                              firstname: String,
                              name: String,
                              age: Int,
                              hiredDate: String
                            ) extends Serializable

/**
  * ToolsCastDataframeIntegrationSpec
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class ToolsCastDataframeIntegrationSpec extends FeatureSpec with SparkSqlSpec with Matchers {

  val expectedStructType = StructType(Array(
    StructField("matricule", StringType, nullable = true),
    StructField("firstName", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = false),
    StructField("hiredDate", DateType, nullable = true),
    StructField("DLL_VALIDITE", IntegerType, nullable = false),
    StructField("DLL_CHECKSUM", IntegerType, nullable = false), // TODO: check nullable default between 1.6 and 2.3
    StructField("DLL_DATE_MAJ", TimestampType, nullable = true),
    StructField("DLL_DATE_CREATION", TimestampType, nullable = true)
  ))
  private val employees = Array(
    Employee("123234877", "Michael", "Rogers", 14, "2014-05-12 13:56"),
    Employee("152934485", "Anand", "Manikutty", 14, "2014-09-01 13:08"),
    Employee("222364883", "Carol", "Smith", 37, "2014-11-17 13:34"))
  var df: DataFrame = _
  var fieldsMap: Seq[(String, String, String)] = List(
    ("matricule", "", "String"),
    ("firstName", "", "String"),
    ("name", "", "String"),
    ("age", "", "Integer"),
    ("hiredDate", "", "DATE")
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    val _sqlc = sqlc

    import _sqlc.implicits._

    df = sc.sparkContext.parallelize(employees).toDF()
    df.printSchema()
  }

  feature("Compute and Process a dataframe using Tools.castComputeAndProcess method") {

    scenario("With empty computeFieldsMap list and no filter") {
      val res = Tools.castComputeAndProcess(df, fieldsMap, computeFieldsMap = Seq(), filter = null)
      // res.printSchema()
      assert(res.schema == expectedStructType)
    }

    scenario("With null computeFieldsMap list and no filter") {
      val res = Tools.castComputeAndProcess(df, fieldsMap, computeFieldsMap = null, filter = null)
      // res.printSchema()
      assert(res.schema == expectedStructType)
    }

    scenario("With given computeFieldsMap and no filter of [seniority, Integer, datediff(current_date(),hiredDate)]") {
      val computed = Tools.castComputeAndProcess(
        df,
        fieldsMap,
        Seq(("seniority", "Integer", "datediff(current_date(), hiredDate)",None)),
        filter = null
      )
      // computed.show()
      // computed.printSchema()
      val currentSchema = computed.schema

      val newExpectedStructType = StructType(expectedStructType.filter(!_.name.contains("DLL_")))
        .add(StructField("seniority", IntegerType, nullable = true))
        .add(StructField("DLL_VALIDITE", IntegerType, nullable = false))
        .add(StructField("DLL_CHECKSUM", IntegerType, nullable = false))
        .add(StructField("DLL_DATE_MAJ", TimestampType, nullable = true))
        .add(StructField("DLL_DATE_CREATION", TimestampType, nullable = true))

      assert(currentSchema.length == 10)
      assert(computed.columns.contains("seniority"))
      assert(currentSchema == newExpectedStructType)
    }
  }
}
