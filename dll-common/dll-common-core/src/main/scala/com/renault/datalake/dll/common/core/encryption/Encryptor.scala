package com.renault.datalake.dll.common.core.encryption

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.renault.datalake.dll.common.core.util.Configs
import org.apache.commons.codec.binary.Base64

object Encryptor {

  def encryptWithParameters(key: String, message: String): String = {

    val skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    val iv = cipher.getParameters.getParameterSpec(classOf[IvParameterSpec])

    cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)

    val encrypted = cipher.doFinal(message.getBytes())

    val messageEncrypted = Base64.encodeBase64String(iv.getIV ++ encrypted)

    messageEncrypted
  }

  def encrypt(message: String): String = {
    val key = Configs.getString("encrypt.key")

    encryptWithParameters(key, message)
  }
}
