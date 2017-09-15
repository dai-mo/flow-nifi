package org.dcs.flow.nifi


import org.dcs.commons.config.{GlobalConfiguration, GlobalConfigurator}
import org.dcs.commons.error.{ErrorConstants, HttpErrorResponse}
import org.dcs.commons.serde.YamlSerializerImplicits._
import org.dcs.commons.ws.ApiConfig

object NifiApiConfig {
  // FIXME: Change global config to add nifi ui url
  val BaseUiUrl = "http://dcs-flow:8090/nifi"
  val BaseApiUrl = GlobalConfigurator.config.toObject[GlobalConfiguration].nifiBaseUrl


  val ClientIdKey = "clientId"
}

trait NifiApiConfig extends ApiConfig {
  import NifiApiConfig._


  override def baseUrl():String = BaseApiUrl

  override def error(status: Int, message: String): HttpErrorResponse = (status match {
    case 400 => ErrorConstants.DCS301
    case 401 => ErrorConstants.DCS302
    case 403 => ErrorConstants.DCS303
    case 404 => ErrorConstants.DCS304
    case 409 => ErrorConstants.DCS305
    case _ => {
      val er = ErrorConstants.DCS001
      er.withDescription(message)
      er
    }
  }).http(status)

}