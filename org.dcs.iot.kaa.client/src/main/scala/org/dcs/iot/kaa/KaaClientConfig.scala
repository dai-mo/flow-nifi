package org.dcs.iot.kaa

import java.io.{File, FileInputStream, InputStream}

import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.dcs.commons.Control
import org.dcs.commons.config.{Configurator, GlobalConfigurator}
import org.dcs.commons.serde.YamlSerializerImplicits._
import org.dcs.iot.kaa.KaaClientConfig.ConfigDirPath

import scala.beans.BeanProperty

/**
  * Handles configuration for the [[org.dcs.iot.kaa.KaaIoTClient Kaa IoT Client]].
  *
  * @author cmathew
  */

case class UserCredentials(@BeanProperty firstName: String,
                           @BeanProperty lastName: String,
                           @BeanProperty userName: String,
                           @BeanProperty password: String,
                           @BeanProperty email: String) {
  def this() = this("","","","","")
}

case class TenantCredentials(@BeanProperty name: String,
                             @BeanProperty admin: UserCredentials,
                             @BeanProperty dev: UserCredentials) {
  def this() = this("", new UserCredentials(), new UserCredentials())
}

case class KaaCredentials(@BeanProperty admin: UserCredentials,
                          @BeanProperty tenant: TenantCredentials) {
  def this() = this(new UserCredentials(), new TenantCredentials())
}

case class ApplicationSchemaConfig(@BeanProperty name: String,
                                   @BeanProperty description: String,
                                   @BeanProperty schemaFile: String) {
  def this() = this("", "", "")

  def schemaFilePath(configDirPath: String): String =
    configDirPath + File.separator + schemaFile

  def schema: String =
    new Schema.Parser()
      .parse(new File(schemaFilePath(ConfigDirPath))).toString()

}

case class LogAppenderConfig(@BeanProperty name: String,
                             @BeanProperty pluginClassName: String,
                             @BeanProperty pluginTypeName: String,
                             @BeanProperty configFile: String) {
  def this() = this("", "", "", "")

  def settingsFilePath(configDirPath: String): String =
    configDirPath + File.separator + configFile

  def settings: String =
    Control
      .using(new FileInputStream(settingsFilePath(KaaClientConfig.ConfigDirPath)))
      { configIS =>
        IOUtils.toString(configIS)
          .replace(System.getProperty("line.separator"), "")
      }
}

case class ApplicationConfig(@BeanProperty name: String,
                             @BeanProperty logSchema: ApplicationSchemaConfig,
                             @BeanProperty configSchema: ApplicationSchemaConfig,
                             @BeanProperty logAppender: LogAppenderConfig) {
  def this() = this("",
    new ApplicationSchemaConfig(),
    new ApplicationSchemaConfig(),
    new LogAppenderConfig())
}


object NifiS2SConfig {

  val DefaultBaseUrl = "http://dcs-flow"
  val DefaultPort = 8090

  def apply(): NifiS2SConfig =
    new NifiS2SConfig(DefaultBaseUrl, DefaultPort, "")

  def apply(inputPortName: String): NifiS2SConfig =
    new NifiS2SConfig(DefaultBaseUrl, DefaultPort, inputPortName)
}

case class NifiS2SConfig(@BeanProperty baseUrl: String,
                         @BeanProperty port: Int,
                         @BeanProperty inputPortName: String) {
  def this() = this("", -1, "")
}



object KaaClientConfig {

  val ConfigDirPath: String = System.getProperty("kaaConfigDir")
//  val credentialsFilePath: Option[String] =
//    Option(ConfigDirPath).map(_ + File.separator + "credentials.json")
  val KaaCredentialsConfigKey = "kaaCredentials"
  val applicationsConfigFilePath: Option[String] =
    Option(ConfigDirPath).map(_ + File.separator + "applications.yaml")


//  val credentials: Option[File] =
//    credentialsFilePath
//      .map { filePath => {
//        val file = new File(filePath)
//        if(file.exists() && file.isFile)
//          file
//        else
//          throw new IllegalArgumentException("Credentials file : " + filePath + "does not exist")
//      }}

  val applicationsConfig: Option[File] =
    applicationsConfigFilePath
      .map { filePath => {
        val file = new File(filePath)
        if(file.exists() && file.isFile)
          file
        else
          throw new IllegalArgumentException("Applications config file : " + filePath + "does not exist")
      }}


  def apply(): KaaClientConfig = {

    new KaaClientConfig(applicationsConfig.map(f => new FileInputStream(f)))
  }
}

/**
  * Encapsulates the DCS Kaa IoT Platform configuration.
  *
  * This class requires the VM properties,
  * -DkaaCredentials=/path/to/DCS Kaa Credentials
  * -DkaaConfigDir=/path/to/DCS Kaa Config Directory> [optional]
  * to be set
  *
  * A sample credentials file is available at src/test/resources/kaaCredentials
  *
  * The Kaa config directory should contain the following files,
  *  - applications.yaml : which contains the configuration for setting up
  *                       the applications, log / config schemas and log
  *                       appender
  *
  *  Any files referenced within the above two config files should also be
  *  placed in the kaaConfigDir.
  *
  *  A sample directory is available at src/test/resources/kaa-config
  *
  * @param applicationConfigIS
  */
class KaaClientConfig(applicationConfigIS: Option[InputStream]) {
  import KaaClientConfig._

  val credentials: KaaCredentials =
    Configurator(KaaCredentialsConfigKey).config().toObject[KaaCredentials]

  val applicationsConfig: Option[List[ApplicationConfig]] =
    applicationConfigIS.map { applicationConfigIS =>
      Control.using(applicationConfigIS) { is =>
        IOUtils.toString(is).toObject[List[ApplicationConfig]]
      }
    }

}
