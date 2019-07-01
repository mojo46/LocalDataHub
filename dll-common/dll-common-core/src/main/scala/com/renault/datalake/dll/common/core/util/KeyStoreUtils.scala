package com.renault.datalake.dll.common.core.util

import java.io._
import java.security.KeyStore
import javax.crypto.spec.PBEKeySpec
import javax.crypto.{SecretKey, SecretKeyFactory}

import com.renault.datalake.dll.common.core.connector.Spark
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * KeyStoreUtils
  *
  * @author #datalake-tooling-support <list.datalake-tooling-support@renault.com>
  */
object KeyStoreUtils {

  lazy val _fsContext = Spark().fsContext

  def main(args: Array[String]): Unit = {
    checkArgs(args)
  }

  def checkArgs(args: Array[String]): Unit = {
    println("Usage for Retrieve Password: KeyStoreUtils  <key alias> <full path to keystore> <keystore password> ")
    println("Usage for Add/Update an entry to KeyStore: KeyStoreUtils  <key alias> <Entry Password> <full path to keystore> <keystore password> ")
    if (args.length != 4) {
      throw new IllegalArgumentException("Usage for Add/Update an entry to KeyStore: KeyStoreUtils  <key alias> <Entry Password> <full path to keystore> <keystore password> ")
    }
  }


  /**
    * Retrieve Password from KeyStore
    *
    * @param entry
    * @param keyStorePassword
    * @param keyStorePath
    * @return
    * @throws Exception
    */
  def getPasswordFromKeystore(entry: String, keyStorePassword: String, keyStorePath: String): String = {

    var ks: KeyStore = KeyStore.getInstance("JCEKS")

    val stream = _fsContext.open(new Path(keyStorePath))

    ks.load(stream, keyStorePassword.toCharArray())

    var keyStorePP: KeyStore.PasswordProtection = new KeyStore.PasswordProtection(keyStorePassword.toCharArray())

    var factory: SecretKeyFactory = SecretKeyFactory.getInstance("PBE")

    var ske: KeyStore.SecretKeyEntry = ks.getEntry(entry, keyStorePP).asInstanceOf[KeyStore.SecretKeyEntry]

    var keySpec: PBEKeySpec = factory.getKeySpec(ske.getSecretKey(), classOf[PBEKeySpec]).asInstanceOf[PBEKeySpec]

    var password: Array[Char] = keySpec.getPassword()

    return new String(password)

  }

  /**
    * Add/Update an entry  Password to KeyStore
    *
    * @param entry
    * @param entryPassword
    * @param keyStoreLocation
    * @param keyStorePassword
    * @throws Exception
    */
  def saveOrUpdatePasswordToKeystore(fsContext: FileSystem, entry: String, entryPassword: String, keyStoreLocation: String, keyStorePassword: String): Unit = {

    var factory: SecretKeyFactory = SecretKeyFactory.getInstance("PBE")

    var generatedSecret: SecretKey = factory.generateSecret(new PBEKeySpec(entryPassword.toCharArray()))

    var keyStorePP: KeyStore.PasswordProtection = new KeyStore.PasswordProtection(keyStorePassword.toCharArray())

    var ks: KeyStore = KeyStore.getInstance("JCEKS")
    ks.load(fsContext.open(new Path(keyStoreLocation)), keyStorePassword.toCharArray())
    ks.setEntry(entry, new KeyStore.SecretKeyEntry(generatedSecret), keyStorePP)
    ks.store(fsContext.create(new Path(keyStoreLocation)), keyStorePassword.toCharArray())
  }
  /**
    * Add/Update an entry  Password to KeyStore
    *
    * @param entry
    * @param entryPassword
    * @param keyStoreLocation
    * @param keyStorePassword
    * @throws Exception
    */
  def saveOrUpdatePasswordToKeystore(entry: String, entryPassword: String, keyStoreLocation: String, keyStorePassword: String, keyStoreModelPath: String): Unit = {

    val factory: SecretKeyFactory = SecretKeyFactory.getInstance("PBE")

    val generatedSecret: SecretKey = factory.generateSecret(new PBEKeySpec(entryPassword.toCharArray()))

    val keyStorePP: KeyStore.PasswordProtection = new KeyStore.PasswordProtection(keyStorePassword.toCharArray())

    val ks: KeyStore = KeyStore.getInstance("JCEKS")
    val inputStream = new FileInputStream(new File(keyStoreModelPath))
    ks.load(inputStream, keyStorePassword.toCharArray())
    ks.setEntry(entry, new KeyStore.SecretKeyEntry(generatedSecret), keyStorePP)
    val outputStream = new FileOutputStream(new File(keyStoreLocation))
    ks.store(outputStream, keyStorePassword.toCharArray())
  }
}
