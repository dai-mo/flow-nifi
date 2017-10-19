package org.dcs.iot.kaa

import org.dcs.commons.error.{ErrorConstants, HttpErrorResponse}
import org.dcs.commons.ws.ApiConfig

/**
  *
  * @author cmathew
  * @constructor
  */
trait KaaApiConfig extends ApiConfig {
  override def baseUrl(): String = "http://dcs-kaaiot:9080/kaaAdmin/rest/api"

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
