package com.renault.datalake.dll.common.core.reporter.mail

import com.renault.datalake.dll.common.core.reporter.Report

/**
  * Structure definition of an Email
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
case class Mail
(
  from: (String, String), // (email -> name)
  to: Seq[String],
  cc: Seq[String] = Seq.empty,
  bcc: Seq[String] = Seq.empty,
  subject: String,
  message: String,
  richMessage: Option[String] = None,
  attachment: Option[java.io.File] = None
) extends Report
