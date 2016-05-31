package org.dcs.flow

import org.dcs.api.model.Processor
import org.dcs.flow.model.ProcessorType

trait ProcessorApi {
  
  def types(): List[ProcessorType]

  def start(processorId: String): Processor
  
}