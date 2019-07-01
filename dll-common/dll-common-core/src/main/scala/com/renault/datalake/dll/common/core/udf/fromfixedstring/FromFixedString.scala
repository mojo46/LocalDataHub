package com.renault.datalake.dll.common.core.udf.fromfixedstring

import scala.util.{Failure, Success, Try}

/**
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com> on 17/01/2019
  */
protected [fromfixedstring] case class FromFixedString(str:String, start:Int, length:Int) {

  protected [fromfixedstring] def parse={
    Try {
      val from= start-1
      val to=if (length == -1) -1 else (start - 1 + length)
      if (to == -1) str.substring(from).trim  else str.substring(from, to).trim
    } match {
      case Failure(e) => "Value not found"
      case Success(r)=>r
    }
  }
}
