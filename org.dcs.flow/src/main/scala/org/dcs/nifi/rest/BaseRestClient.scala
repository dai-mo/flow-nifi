package org.dcs.nifi.rest

import javax.ws.rs.core.Response

trait BaseRestClient {  
  
  def response(path: String): Response
}