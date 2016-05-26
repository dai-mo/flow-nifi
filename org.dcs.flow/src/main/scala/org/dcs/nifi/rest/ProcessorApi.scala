package org.dcs.nifi.rest

import org.dcs.api.model.Processor

trait ProcessorApi extends BaseRestClient {
  
  def types(): List[Processor]
  
}