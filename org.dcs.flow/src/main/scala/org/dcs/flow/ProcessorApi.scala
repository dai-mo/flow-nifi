package org.dcs.flow

import org.dcs.api.model.Processor

trait ProcessorApi {
  
  def types(): List[Processor]
  
}