package org.dcs.flow.nifi

import javax.ws.rs.core.Response

import org.apache.nifi.web.api.entity.{FlowEntity, ProcessGroupFlowEntity, TemplatesEntity}
import org.dcs.api.error.{ErrorConstants, ErrorResponse}
import org.dcs.commons.YamlSerializerImplicits._
import org.dcs.commons.config.{GlobalConfiguration, GlobalConfigurator}
import org.dcs.flow.ApiConfig
import play.api.libs.json.Json

object NifiApiConfig {
  val BaseUrl = GlobalConfigurator.config.toObject[GlobalConfiguration].nifiBaseUrl

  val ClientIdKey = "clientId"
}

trait NifiApiConfig extends ApiConfig {
  import NifiApiConfig._

  
  override def baseUrl():String = BaseUrl

  override def error(status: Int, message: String): ErrorResponse = status match {
    case 400 => ErrorConstants.DCS301
    case 401 => ErrorConstants.DCS302
    case 403 => ErrorConstants.DCS303
    case 404 => ErrorConstants.DCS304
    case 409 => ErrorConstants.DCS305
    case _ => {
      val er = ErrorConstants.DCS001
      er.withErrorMessage(message)
      er
    }
  }

}