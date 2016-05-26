package org.dcs.flow

import org.dcs.api.model.Processor
import org.dcs.flow.BaseRestClient

trait ProcessorApi extends BaseRestClient {
  
  def types(): List[Processor]
  
}