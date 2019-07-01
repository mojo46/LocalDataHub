package com.renault.datalake.dll.common.core.reporter.mail

import com.renault.datalake.dll.common.core.reporter.{Reporter, ReporterProvider}

/**
  * [[ReporterProvider]] implementation for [[EmailReporter]]
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class EmailReporterProvider extends ReporterProvider {

  override def reporterName: String = "reporting"

  override def prepareReporter(conf: Any): Reporter = {
    if (!conf.isInstanceOf[(String, Int)]) {
      throw new IllegalArgumentException(
        s"${this.getClass.getName}.prepareReporter(..) parameter should be (String, Int), current is ${conf.getClass}"
      )
    }

    val (host, port) = conf.asInstanceOf[(String, Int)]
    new EmailReporter(host, port)
  }
}
