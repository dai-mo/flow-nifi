package org.dcs.iot.kaa

import java.net.URLEncoder

import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient

import scala.beans.BeanProperty
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Client for the Kaa IoT Platform REST API
  *
  * @author cmathew
  */

case class Tenant(@BeanProperty id: String,
                  @BeanProperty name: String) {
  def this() = this("", "")
}

case class User(@BeanProperty username: String,
                @BeanProperty tenantId: String,
                @BeanProperty authority: String,
                @BeanProperty firstName: String,
                @BeanProperty lastName: String,
                @BeanProperty mail: String,
                @BeanProperty tempPassword: String)  {
  def this() = this("", "", "", "", "", "", "")
}

case class Application(@BeanProperty id: String,
                       @BeanProperty applicationToken: String,
                       @BeanProperty name: String,
                       @BeanProperty tenantId: String) {
  def this() = this("", "", "", "")
}

case class CTLMetaInfo(@BeanProperty id: String,
                       @BeanProperty tenantId: String,
                       @BeanProperty applicationId: String) {
  def this() = this("", "", "")
}

case class CTLSchema(@BeanProperty id: String,
                     @BeanProperty metaInfo: CTLMetaInfo) {
  def this() = this("", new CTLMetaInfo())
}


case class TenantSchema(@BeanProperty id: String,
                        @BeanProperty fqn: String,
                        @BeanProperty tenantId: String,
                        @BeanProperty applicationId: String,
                        @BeanProperty versions: List[Int]) {
  def this()  = this("", "", "", "", Nil)
}

case class ApplicationSchema(@BeanProperty version: Int,
                             @BeanProperty applicationId: String,
                             @BeanProperty name: String,
                             @BeanProperty description: String,
                             @BeanProperty ctlSchemaId: String) {
  def this() = this(0, "", "", "", "")
}

case class LogAppender(@BeanProperty id: String,
                       @BeanProperty pluginClassName: String,
                       @BeanProperty pluginTypeName: String,
                       @BeanProperty name: String,
                       @BeanProperty description: String,
                       @BeanProperty applicationId: String,
                       @BeanProperty applicationToken: String,
                       @BeanProperty tenantId: String,
                       @BeanProperty headerStructure: List[String],
                       @BeanProperty confirmDelivery: Boolean,
                       @BeanProperty jsonConfiguration: String,
                       @BeanProperty minLogSchemaVersion: Int = 1,
                       @BeanProperty maxLogSchemaVersion: Int = Int.MaxValue) {
  def this() = this("","", "", "", "", "", "", "", Nil, true, "")

  def withSettings(settings: String): LogAppender =
    LogAppender(id,
      pluginClassName,
      pluginTypeName,
      name,
      description,
      applicationId,
      applicationToken,
      tenantId,
      headerStructure,
      confirmDelivery,
      settings,
      minLogSchemaVersion,
      maxLogSchemaVersion)
}

case class LogAppenderSettings(@BeanProperty id: String,
                               @BeanProperty pluginClassName: String,
                               @BeanProperty applicationId: String,
                               @BeanProperty jsonConfiguration: String) {
  def this() = this("", "", "", "")
}

case class SDKProfile(@BeanProperty id: String,
                      @BeanProperty name: String,
                      @BeanProperty applicationId: String,
                      @BeanProperty applicationToken: String)


object KaaIoTClient {

  val getApplicationsPath: String = "/applications"
  def applicationTokenPath(applicationToken: String): String = "/application/" + applicationToken

  def logAppendersPath(applicationToken: String) :String = "/logAppenders/" + applicationToken
  val logAppenderPath:String = "/logAppender"
  val deleteLogAppenderPath: String = "/delLogAppender"

  def applicationLogSchemaPath(applicationToken: String): String = "/logSchemas/" + applicationToken
  val flatCtlSchemaPath: String = "/CTL/getFlatSchemaByCtlSchemaId"

  val kaaClientConfig = KaaClientConfig()
  val credentials = kaaClientConfig.credentials

  object KaaAdminClient extends JerseyRestClient with KaaApiConfig
  object KaaTenantAdminClient extends JerseyRestClient with KaaApiConfig
  object KaaTenantDevClient extends JerseyRestClient with KaaApiConfig


  KaaAdminClient.auth(credentials.admin.userName, credentials.admin.password)
  KaaTenantAdminClient.auth(credentials.tenant.admin.userName, credentials.tenant.admin.password)
  KaaTenantDevClient.auth(credentials.tenant.dev.userName, credentials.tenant.dev.password)


  def apply(): KaaIoTClient = {
    new KaaIoTClient()
  }

}

class KaaIoTClient {
  import KaaIoTClient._

  def applications(): Future[List[Application]] = {
    KaaTenantDevClient.getAsJson(path = getApplicationsPath)
      .map { response =>
        response.toObject[List[Application]]
      }
  }

  def application(applicationToken: String): Future[Application] =
    KaaTenantDevClient.getAsJson(path = applicationTokenPath(applicationToken))
    .map(_.toObject[Application])

  def createLogAppender(application: Application, logAppender: LogAppenderConfig): Future[LogAppender] = {
    createLogAppender(application,
      logAppender.name,
      logAppender.pluginClassName,
      logAppender.pluginTypeName,
      logAppender.settings)
  }


  def createLogAppender(application: Application,
                        name: String,
                        pluginClassName: String,
                        pluginTypeName: String,
                        settings: String): Future[LogAppender] = {
    KaaTenantDevClient.postAsJson(path = logAppenderPath,
      body = LogAppender("",
        pluginClassName,
        pluginTypeName,
        name,
        name,
        application.id,
        application.applicationToken,
        application.tenantId,
        List("KEYHASH", "VERSION", "TIMESTAMP", "TOKEN", "LSVERSION"),
        true,
        settings))
      .map(_.toObject[LogAppender])
  }

  def updateLogAppenderSettings(logAppender: LogAppender,
                                settings: String): Future[LogAppender] =
    KaaTenantDevClient.postAsJson(path = logAppenderPath,
      body = logAppender.withSettings(settings))
      .map(_.toObject[LogAppender])


  def removeLogAppender(logAppenderId: String): Future[Boolean] = {
    KaaTenantDevClient.postAsJson(path = deleteLogAppenderPath,
      queryParams = List(("logAppenderId", logAppenderId)))
      .map(response => true)
  }

  def logAppenderWithRootInputPortId(applicationToken: String, rootInputPortId: String): Future[Option[LogAppender]] = {
    KaaTenantDevClient.getAsJson(path = logAppendersPath(applicationToken))
      .map(response => {
        val las = response.toObject[List[LogAppender]]
        las.find(la => la.jsonConfiguration.toObject[NifiS2SConfig].inputPortName == rootInputPortId)
      })
  }

  def removeLogAppenderWithRootInputPortId(applicationToken: String, rootInputPortId: String): Future[Boolean] = {
    logAppenderWithRootInputPortId(applicationToken, rootInputPortId)
      .flatMap(_.map(la => removeLogAppender(la.id)).getOrElse(Future(false)))
  }

  def logSchemaWithMaxVersion(applicationToken: String): Future[ApplicationSchema] = {
    KaaTenantDevClient.getAsJson(path = applicationLogSchemaPath(applicationToken))
      .map(_.toObject[List[ApplicationSchema]].maxBy(_.version))
  }

  def ctlSchema(schemaId: String): Future[String] = {
    KaaTenantDevClient.getAsJson(path = flatCtlSchemaPath,
      queryParams = List(("id", schemaId)))
  }

  def maxApplicationLogSchema(applicationToken: String): Future[String] = {
    logSchemaWithMaxVersion(applicationToken)
      .flatMap(s => ctlSchema(s.ctlSchemaId))
  }



}
