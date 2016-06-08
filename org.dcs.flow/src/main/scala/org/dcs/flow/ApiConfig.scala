package org.dcs.flow

import javax.ws.rs.core.Response

import org.dcs.api.error.ErrorResponse

trait ApiConfig {
  
  def baseUrl():String

  def error(response: Response): ErrorResponse

  
}