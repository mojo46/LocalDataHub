package com.renault.datalake.dll.common.core.udf.fromfixedstring

import org.apache.spark.sql.functions.{col, explode, lit, udf}
import org.apache.spark.sql.{Column, DataFrame}

import scala.language.implicitConversions

/**
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com> on 17/01/2019
  */
case class FromFixed(fields:Seq[FieldInfo]) {
  // UDF to extract i-th element from array column
private  val elem = udf((x: String,len:Int) =>x.grouped(len).toSeq.map(f=>SeqToString(f).elt))
 private  val  parseString=udf((e:String,from:Int,to:Int)=>FromFixedString(e,from,to).parse)

  // Method to apply 'elem' UDF on each element, requires knowing length of sequence in advance
 private def split(col: Column): Seq[Column] =
    fields.map(f=>parseString(col,lit(f.start),lit(f.length)).as(f.name))


  def from_fixed(df:DataFrame,colName:String,strLen:Int)={
    df.withColumn(colName.concat("_from_fixed"),elem(col(colName), lit(strLen)))
      .withColumn(colName.concat("_init"),explode(col(colName.concat("_from_fixed"))))
      .select(col(colName),col(colName.concat("_init")),split(col(colName.concat("_init"))))
  }

 // Implicit conversion to make things nicer to use, e.g.
  // select(Column, Seq[Column], Column) is converted into select(Column*) flattening sequences
 private implicit class DataFrameSupport(df: DataFrame) {
   def select(cols: Any*): DataFrame = {
      var buffer: Seq[Column] = df.columns.map(col(_))
      for (col <- cols) {
        if (col.isInstanceOf[Seq[_]]) {
          buffer = buffer ++ col.asInstanceOf[Seq[Column]]
        } else {
          buffer = buffer :+ col.asInstanceOf[Column]
        }
      }
      df.select(buffer:_*)
    }
  }

}

/**
  * Get Column element on ArrayListType as String
  * @param elt
  */
private case class SeqToString(elt: String)

/**
  * Description of computed fields
  * @param name
  * @param start
  * @param length
  */
case class FieldInfo(name: String, start: Int, length: Int)
