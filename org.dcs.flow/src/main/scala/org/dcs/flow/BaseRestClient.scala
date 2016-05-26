package org.dcs.flow

import javax.ws.rs.core.Response

trait BaseRestClient {  
  
  def response(path: String): Response
}