package org.dcs.flow

import org.dcs.api.error.ErrorResponse


trait ApiConfig {
  
  def baseUrl():String

  def error(status: Int, message: String): ErrorResponse

  def endpoint(path: String): String = {
    baseUrl() + path
  }
  
}