package com.renault.datalake.dll.common.core.reporter.mail

import com.renault.datalake.dll.common.core.reporter.{Report, Reporter}
import org.apache.commons.mail._

/**
  * Email implementation of [[Reporter]]
  * Send one or more [[Mail]] through given SMTP server informations
  * Logic taken from https://gist.github.com/mariussoutier/3436111
  *
  * @param host [[String]] SMTP host
  * @param port [[Int]] SMTP port
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
class EmailReporter(host: String, port: Int) extends Reporter {

  val PLAIN: String = "PLAIN"
  val MULTIPART: String = "MULTIPART"
  val RICH: String = "RICH"

  def send(mails: Seq[Mail]): Unit = {
    mails.foreach(m => send(m))
  }

  override def send(report: Report): String = {
    val mail = if (!report.isInstanceOf[Mail]) {
      throw new IllegalArgumentException(
        s"${this.getClass.getSimpleName}.send(..) should receive an instance of ${Mail.getClass.getName}"
      )
    } else report.asInstanceOf[Mail]

    val format =
      if (mail.attachment.isDefined) MULTIPART
      else if (mail.richMessage.isDefined) RICH
      else PLAIN

    val commonsMail: Email = format match {
      case PLAIN => new SimpleEmail().setMsg(mail.message)
      case RICH => new HtmlEmail().setHtmlMsg(mail.richMessage.get).setTextMsg(mail.message)
      case MULTIPART =>
        val attachment = new EmailAttachment()
        attachment.setPath(mail.attachment.get.getAbsolutePath)
        attachment.setDisposition(EmailAttachment.ATTACHMENT)
        attachment.setName(mail.attachment.get.getName)
        new MultiPartEmail().attach(attachment).setMsg(mail.message)
    }

    commonsMail.setHostName(host)
    commonsMail.setSmtpPort(port)
    commonsMail.setTLS(false)

    // Can't add these via fluent API because it produces exceptions
    mail.to foreach commonsMail.addTo
    if (mail.cc != null) mail.cc foreach commonsMail.addCc
    if (mail.bcc != null) mail.bcc foreach commonsMail.addBcc

    commonsMail
      .setFrom(mail.from._1, mail.from._2)
      .setSubject(mail.subject)
      .send()
  }
}
