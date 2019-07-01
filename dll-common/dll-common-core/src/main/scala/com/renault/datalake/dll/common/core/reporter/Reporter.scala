package com.renault.datalake.dll.common.core.reporter

/**
  * Generic reporter interface
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
trait Reporter {
  def send(report: Report): Any
}
