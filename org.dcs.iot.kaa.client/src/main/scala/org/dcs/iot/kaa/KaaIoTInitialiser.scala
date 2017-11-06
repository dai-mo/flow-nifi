package org.dcs.iot.kaa

import java.net.URLEncoder

import org.dcs.commons.ws.JerseyRestClient
import org.dcs.iot.kaa.KaaIoTClient._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import org.dcs.commons.serde.JsonSerializerImplicits._
import scala.concurrent.ExecutionContext.Implicits._

/**
  *
  * @author cmathew
  * @constructor
  */
object KaaIoTInitialiser  extends KaaIoTClient {
  val KaaAdminRole = "KAA_ADMIN"
  val TenantAdminRole = "TENANT_ADMIN"
  val TenantDevRole = "TENANT_DEVELOPER"
  val TenantUserRole = "TENANT_USER"

  val createKaaAdminPath: String = "/auth/createKaaAdmin"
  val changeUserPasswordPath: String = "/auth/changePassword"
  val applicationPath: String = "/application"

  val uploadSchemaPath: String = "/CTL/saveSchema"
  val deleteSchemaPath: String = "/CTL/deleteSchema"
  val createLogSchemaPath: String = "/saveLogSchema"
  val createConfigSchemaPath: String = "/saveConfigurationSchema"

  val createSdkProfilePath = "/createSdkProfile"

  val tenantSchemasPath: String = "/CTL/getTenantSchemas"
  val tenantPath: String = "/tenant"
  val tenantsPath: String = "/tenants"
  val userPath: String = "/user"

  val applicationsConfig: List[ApplicationConfig] = kaaClientConfig.applicationsConfig match {
    case Some(value) => value
    case None => throw new IllegalArgumentException("Application config not available")
  }

  def apply(): KaaIoTInitialiser = new KaaIoTInitialiser()

  def main(args: Array[String]): Unit = {
    val kaaIoTInitialiser = KaaIoTInitialiser()
    Await.ready(kaaIoTInitialiser.setupCredentials()
      .flatMap(response => kaaIoTInitialiser.createApplications()),
      Duration.Inf)

// FIXME: Creating SDK Profile does not seem to work
//    Await.ready(applications()
//      .map(_.find(_.name == "Heartbeat Monitor"))
//      .flatMap(app => kaaIoTInitialiser.createSdkProfile("Default SDK", app.get.id, app.get.applicationToken)),
//      Duration.Inf)
  }
}

class KaaIoTInitialiser {
  import KaaIoTInitialiser._

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

  def createApplications(): Future[Boolean] = {

    KaaAdminClient.getAsJson(path = tenantsPath)
      .flatMap { response =>
        val tenants = response.asList[Tenant]
        tenants.find(_.name == credentials.tenant.name)
          .map { tenant =>
            Future.sequence(applicationsConfig.map { applicationConfig =>
              KaaTenantAdminClient.postAsJson(path = applicationPath,
                body = Application("",
                  "",
                  applicationConfig.name,
                  tenant.id))
                .map(response => response.toObject[Application])
                .flatMap { application =>
                  KaaTenantDevClient.postAsJson(path = uploadSchemaPath,
                    queryParams = List(
                      ("body", URLEncoder.encode(applicationConfig.logSchema.schema, "UTF-8")),
                      ("tenantId", tenant.id)))
                    .map(response => response.toObject[CTLSchema])
                    .flatMap { ctlLogSchema =>
                      KaaTenantDevClient.postAsJson(path = createLogSchemaPath,
                        body = ApplicationSchema(-1,
                          application.id,
                          applicationConfig.logSchema.name,
                          applicationConfig.logSchema.name,
                          ctlLogSchema.id))
                    }
                    .flatMap { response =>
                      KaaTenantDevClient.postAsJson(path = uploadSchemaPath,
                        queryParams = List(
                          ("body", URLEncoder.encode(applicationConfig.configSchema.schema, "UTF-8")),
                          ("tenantId", tenant.id)))
                        .map(response => response.toObject[CTLSchema])
                        .flatMap { ctlConfigSchema =>
                          KaaTenantDevClient.postAsJson(path = createConfigSchemaPath,
                            body = ApplicationSchema(-1,
                              application.id,
                              applicationConfig.configSchema.name,
                              applicationConfig.configSchema.name,
                              ctlConfigSchema.id))
                        }
                    }
                    .flatMap { response =>
                      if(applicationConfig.logAppender != null)
                        createLogAppender(application, applicationConfig.logAppender)
                      else
                        Future("")
                    }
                }
            })
              .map(response => true)
          }
          .getOrElse(Future(false))
      }
  }


  def createSdkProfile(name: String,
                       applicationId: String,
                       applicationToken: String): Future[SDKProfile] = {
    KaaTenantDevClient.postAsJson(path = createSdkProfilePath,
      body = SDKProfile("", name, applicationId, applicationToken))
      .map(_.toObject[SDKProfile])
  }

}
