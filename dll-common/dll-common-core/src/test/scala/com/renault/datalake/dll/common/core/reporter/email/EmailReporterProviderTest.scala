package com.renault.datalake.dll.common.core.reporter.email

import com.renault.datalake.dll.common.core.reporter.mail.EmailReporter
import com.renault.datalake.dll.common.core.reporter.{ReporterProvider, ReporterProviderException}
import org.scalatest.{FlatSpec, Matchers}

/**
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class EmailReporterProviderTest extends FlatSpec with Matchers {
  val host = "0.0.0.0"
  val port: Int = 2222

  "The ReporterProvider" should "return the proper class" in {
    ReporterProvider.getReporter("reporting", (host, port)).getClass should be(classOf[EmailReporter])
  }

  it should "throws a ReporterProviderException" in {
    // Wrong job
    assertThrows[ReporterProviderException]{
      ReporterProvider.getReporter("UnknownReporter", None)
    }

    // Missing params
    assertThrows[ReporterProviderException]{
      ReporterProvider.getReporter("reporting", None)
    }

    // Wrong params
    assertThrows[ReporterProviderException]{
      ReporterProvider.getReporter("reporting", s"$host:$port")
    }
  }
}
