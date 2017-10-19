package org.dcs.iot.kaa

import java.io.{File, FileInputStream, InputStream}

import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.dcs.commons.Control
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.beans.BeanProperty

/**
  * Handles configuration for the [[org.dcs.commons.iot.kaa.KaaIoTClient Kaa IoT Client]].
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

}

case class LogAppenderConfig(@BeanProperty name: String,
                             @BeanProperty pluginClassName: String,
                             @BeanProperty pluginTypeName: String,
                             @BeanProperty configFile: String) {
  def this() = this("", "", "", "")

  def configFilePath(configDirPath: String): String =
    configDirPath + File.separator + configFile
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



object KaaClientConfig {

  val ConfigDirPath: String = System.getProperty("kaaConfigDir")
  val credentialsFilePath: Option[String] =
    Option(ConfigDirPath).map(_ + File.separator + "credentials.json")
  val applicationConfigFilePath: Option[String] =
    Option(ConfigDirPath).map(_ + File.separator + "application.json")


  val credentials: Option[File] =
    credentialsFilePath
      .map { filePath => {
        val file = new File(filePath)
        if(file.exists() && file.isFile)
          file
        else
          throw new IllegalArgumentException("Credentials file : " + filePath + "does not exist")
      }}

  val applicationConfig: Option[File] =
    applicationConfigFilePath
      .map { filePath => {
        val file = new File(filePath)
        if(file.exists() && file.isFile)
          file
        else
          throw new IllegalArgumentException("Application config file : " + filePath + "does not exist")
      }}


  def apply(): KaaClientConfig = {

    new KaaClientConfig(credentials.map(f => new FileInputStream(f)),
      applicationConfig.map(f => new FileInputStream(f)))
  }
}

/**
  * Encapsulates the DCS Kaa IoT Platform configuration.
  *
  * This class requires the VM property,
  * -DkaaConfigDir=/path/to/DCS Kaa Config Directory>
  * to be set
  *
  * The directory should contain the following files,
  *  - credentials.json : which contains the credentials for
  *                       kaa superuser, tenant admin and tenant dev
  *  - application.json : which contains the configuration for setting up
  *                       the application, log / config schemas and log
  *                       appender
  *
  *  Any files referenced within the above two config files should also be
  *  placed in the kaaConfigDir.
  *
  *  A sample directory is available at src/test/resources/kaa-config
  *
  * @param credentialsIS
  * @param applicationConfigIS
  */
class KaaClientConfig(credentialsIS: Option[InputStream],
                      applicationConfigIS: Option[InputStream]) {
  import KaaClientConfig._

  val credentials: Option[KaaCredentials] =
    credentialsIS.map { credentialsIS =>
      Control.using(credentialsIS) { is =>
        IOUtils.toString(is).toObject[KaaCredentials]
      }
    }

  val applicationConfig: Option[ApplicationConfig] =
    applicationConfigIS.map { applicationConfigIS =>
      Control.using(applicationConfigIS) { is =>
        IOUtils.toString(is).toObject[ApplicationConfig]
      }
    }


  def logSchema(): String =
    applicationConfig.map(ac =>
      new Schema.Parser().parse(new File(ac.logSchema.schemaFilePath(ConfigDirPath))).toString())
      .getOrElse(throw new IllegalArgumentException("Log Schema file not available"))

  def configSchema():String =
    applicationConfig.map(ac =>
      new Schema.Parser().parse(new File(ac.configSchema.schemaFilePath(ConfigDirPath))).toString())
      .getOrElse(throw new IllegalArgumentException("Config Schema file not available"))

  def logAppenderConfig(): String =
    applicationConfig.map(ac =>
      Control
        .using(new FileInputStream(ac.logAppender.configFilePath(ConfigDirPath))) { mongoDbConfigIS =>
          IOUtils.toString(mongoDbConfigIS)
            .replace(System.getProperty("line.separator"), "")
        }
    )
      .getOrElse(throw new IllegalArgumentException("Log Appender Config file not available"))
}
