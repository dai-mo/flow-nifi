package org.dcs.flow.nifi

import javax.ws.rs.core.Response

import org.dcs.api.error.{ErrorConstants, ErrorResponse}
import org.dcs.commons.config.ConfigurationFacade
import org.dcs.flow.ApiConfig

object NifiApiConfig {
  val BaseUrl = ConfigurationFacade.config.nifiBaseUrl  
}

trait NifiApiConfig extends ApiConfig {
  import NifiApiConfig._
  
  override def baseUrl():String = BaseUrl

  override def error(response: Response): ErrorResponse = response.getStatus match {
    case 400 => ErrorConstants.DCS301
    case 401 => ErrorConstants.DCS302
    case 403 => ErrorConstants.DCS303
    case 404 => ErrorConstants. DCS304
    case 409 => ErrorConstants.DCS305
    case _ => {
      val er = ErrorConstants.DCS001
      er.errorMessage = response.readEntity(classOf[String])
      er
    }
  }

}