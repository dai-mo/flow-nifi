package org.dcs.nifi.rest

import javax.ws.rs.core.Response

trait MockNifiProcessorApi extends NifiProcessorApi {
  
  var response: Response = _
  
  override def response(path: String): Response
  
}