package org.dcs.flow.nifi

import javax.ws.rs.core.Response

import org.dcs.api.model.{ErrorConstants, ErrorResponse}
import org.dcs.commons.config.ConfigurationFacade
import org.dcs.flow.ApiConfig

object NifiApiConfig {
  val BaseUrl = ConfigurationFacade.config.nifiBaseUrl  
}

trait NifiApiConfig extends ApiConfig {
  import NifiApiConfig._
  
  override def baseUrl():String = BaseUrl

  override def error(response: Response): ErrorResponse = response.getStatus match {
    case 400 => errorWithNifiMessage(response, "DCS301")
    case 401 => errorWithNifiMessage(response, "DCS302")
    case 403 => errorWithNifiMessage(response, "DCS303")
    case 404 => errorWithNifiMessage(response, "DCS304")
    case 409 => errorWithNifiMessage(response, "DCS305")
    case _ => errorWithNifiMessage(response, "DCS001")
  }

  def errorWithNifiMessage(response: Response, code: String): ErrorResponse = {
    val er = ErrorConstants.getErrorResponse(code)
    println(response.readEntity(classOf[String]))
    er
  }
}