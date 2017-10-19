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

case class ApplicationSchema(@BeanProperty version: String,
                             @BeanProperty applicationId: String,
                             @BeanProperty name: String,
                             @BeanProperty description: String,
                             @BeanProperty ctlSchemaId: String) {
  def this() = this("", "", "", "", "")
}

case class LogAppender(@BeanProperty pluginClassName: String,
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
  def this() = this("", "", "", "", "", "", "", Nil, true, "")
}


object KaaIoTClient {
  val KaaAdminRole = "KAA_ADMIN"
  val TenantAdminRole = "TENANT_ADMIN"
  val TenantDevRole = "TENANT_DEVELOPER"
  val TenantUserRole = "TENANT_USER"

  val createKaaAdminPath: String = "/auth/createKaaAdmin"
  val changeUserPasswordPath: String = "/auth/changePassword"
  val applicationPath: String = "/application"
  val getApplicationsPath: String = "/applications"
  val uploadSchemaPath: String = "/CTL/saveSchema"
  val deleteSchemaPath: String = "/CTL/deleteSchema"
  val createLogSchemaPath: String = "/saveLogSchema"
  val createConfigSchemaPath: String = "/saveConfigurationSchema"
  val createLogAppender:String = "/logAppender"
  val tenantSchemasPath: String = "/CTL/getTenantSchemas"
  val tenantPath: String = "/tenant"
  val tenantsPath: String = "/tenants"
  val userPath: String = "/user"

  val kaaClientConfig = KaaClientConfig()
  val credentials = kaaClientConfig.credentials match {
    case Some(value) => value
    case None => throw new IllegalArgumentException("Credentials not available")
  }
  val applicationConfig = kaaClientConfig.applicationConfig match {
    case Some(value) => value
    case None => throw new IllegalArgumentException("Application config not available")
  }

  object KaaAdminClient extends JerseyRestClient with KaaApiConfig
  object KaaTenantAdminClient extends JerseyRestClient with KaaApiConfig
  object KaaTenantDevClient extends JerseyRestClient with KaaApiConfig


  KaaAdminClient.auth(credentials.admin.userName, credentials.admin.password)
  KaaTenantAdminClient.auth(credentials.tenant.admin.userName, credentials.tenant.admin.password)
  KaaTenantDevClient.auth(credentials.tenant.dev.userName, credentials.tenant.dev.password)


  def apply(): KaaIoTClient = {
    new KaaIoTClient()
  }

  def main(args: Array[String]): Unit = {
    val kaaIoTClient = KaaIoTClient()
    Await.ready(kaaIoTClient.setupCredentials()
      .flatMap(response => kaaIoTClient.createApplication()),
      Duration.Inf)
  }

}

class KaaIoTClient {
  import KaaIoTClient._

  def setupCredentials(): Future[Boolean] = {
    object BaseClient extends JerseyRestClient with KaaApiConfig

    BaseClient.postAsJson(path = createKaaAdminPath,
      queryParams = List(
        ("username", credentials.admin.userName),
        ("password", credentials.admin.password)))
      .flatMap { response =>
        KaaAdminClient.postAsJson(path = tenantPath,
          body = Tenant("", credentials.tenant.name))
      }
      .flatMap { response =>
        val dcsTenant = response.toObject[Tenant]
        KaaAdminClient.postAsJson(path = userPath,
          body = User(credentials.tenant.admin.userName,
            dcsTenant.id,
            TenantAdminRole,
            credentials.tenant.admin.firstName,
            credentials.tenant.admin.lastName,
            credentials.tenant.admin.email,
            credentials.tenant.admin.password))
          .flatMap { response =>
            val tempPassword = response.toObject[User].tempPassword
            KaaAdminClient.postAsJson(path = changeUserPasswordPath,
              queryParams = List(
                ("username", credentials.tenant.admin.userName),
                ("oldPassword", tempPassword),
                ("newPassword", credentials.tenant.admin.password))
            )
          }
          .flatMap { response =>
            KaaTenantAdminClient.postAsJson(path = userPath,
              body = User(credentials.tenant.dev.userName,
                dcsTenant.id,
                TenantDevRole,
                credentials.tenant.dev.firstName,
                credentials.tenant.dev.lastName,
                credentials.tenant.dev.email,
                credentials.tenant.dev.password))
              .flatMap { response =>
                val tempPassword = response.toObject[User].tempPassword
                KaaTenantAdminClient.postAsJson(path = changeUserPasswordPath,
                  queryParams = List(
                    ("username", credentials.tenant.dev.userName),
                    ("oldPassword", tempPassword),
                    ("newPassword", credentials.tenant.dev.password))
                )
              }
          }
      }
      .map(response => true)
  }

  def createApplication(): Future[Boolean] = {

    KaaAdminClient.getAsJson(path = tenantsPath)
      .flatMap { response =>
        val tenants = response.asList[Tenant]
        tenants.find(_.name == credentials.tenant.name)
          .map { tenant =>
            KaaTenantAdminClient.postAsJson(path = applicationPath,
              body = Application("",
                "",
                applicationConfig.name,
                tenant.id))
              .map(response => response.toObject[Application])
              .flatMap { application =>
                KaaTenantDevClient.postAsJson(path = uploadSchemaPath,
                  queryParams = List(
                    ("body", URLEncoder.encode(kaaClientConfig.logSchema(), "UTF-8")),
                    ("tenantId", tenant.id)))
                  .map(response => response.toObject[CTLSchema])
                  .flatMap {  ctlLogSchema =>
                    KaaTenantDevClient.postAsJson(path = createLogSchemaPath,
                      body = ApplicationSchema("",
                        application.id,
                        applicationConfig.logSchema.name,
                        applicationConfig.logSchema.name,
                        ctlLogSchema.id))
                  }
                  .flatMap { response =>
                    KaaTenantDevClient.postAsJson(path = uploadSchemaPath,
                      queryParams = List(
                        ("body", URLEncoder.encode(kaaClientConfig.configSchema(), "UTF-8")),
                        ("tenantId", tenant.id)))
                      .map(response => response.toObject[CTLSchema])
                      .flatMap { ctlConfigSchema =>
                        KaaTenantDevClient.postAsJson(path = createConfigSchemaPath,
                          body = ApplicationSchema("",
                            application.id,
                            applicationConfig.configSchema.name,
                            applicationConfig.configSchema.name,
                            ctlConfigSchema.id))
                      }
                  }
                  .flatMap { response =>
                    KaaTenantDevClient.postAsJson(path = createLogAppender,
                      body = LogAppender(applicationConfig.logAppender.pluginClassName,
                        applicationConfig.logAppender.pluginTypeName,
                        applicationConfig.logAppender.name,
                        applicationConfig.logAppender.name,
                        application.id,
                        application.applicationToken,
                        application.tenantId,
                        List("KEYHASH", "VERSION", "TIMESTAMP", "TOKEN", "LSVERSION"),
                        true,
                        kaaClientConfig.logAppenderConfig()))
                  }
              }
              .map(response => true)
          }
          .getOrElse(Future(false))
      }


  }

}
