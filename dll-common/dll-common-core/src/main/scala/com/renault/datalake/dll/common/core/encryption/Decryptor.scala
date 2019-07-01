package com.renault.datalake.dll.common.core.encryption

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import com.renault.datalake.dll.common.core.util.Configs
import org.apache.commons.codec.binary.Base64

object Decryptor {

  private def decryptWithParameters(key: String, encryptedMessage: String, ivLength: Int): String = {
    val keySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    val encryptedMessage64 = Base64.decodeBase64(encryptedMessage)
    val iv = encryptedMessage64.slice(0, ivLength)
    val messageToDecrypt = encryptedMessage64.slice(ivLength, encryptedMessage64.length + 1)

    cipher.init(Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(iv))

    new String(cipher.doFinal(messageToDecrypt))
  }

  def decrypt(encryptedMessage: String): String = {
    val key = Configs.getString("encrypt.key")

    decryptWithParameters(key, encryptedMessage, 16)
  }
}
