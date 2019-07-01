package com.renault.datalake.dll.common.core.security

import com.renault.datalake.dll.common.core.encryption.{Decryptor, Encryptor}
import org.apache.log4j.Logger
import org.scalatest.FunSuite

class DecryptorTest extends FunSuite{


    test("testDecrypt") {
      System.setProperty("config.resource", this.getClass.getClassLoader.getResource("reference.conf").getFile)
      lazy val logger = Logger.getLogger(this.getClass.getName)
    val messageToEncrypt = "LogicC08"
    val messageEncrypted = Encryptor.encrypt(messageToEncrypt)
    logger.info("###messageEncrypted  "  + messageEncrypted)

    logger.info(Decryptor.decrypt(messageEncrypted))
    assert(Decryptor.decrypt(messageEncrypted) == "LogicC08")
  }
}
