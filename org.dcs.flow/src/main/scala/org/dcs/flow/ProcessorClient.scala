package org.dcs.flow

import org.dcs.api.model.Processor
import org.dcs.flow.model.ProcessorType

trait ProcessorClient {
  
  def types(): List[ProcessorType]

  def start(processorId: String): Processor
  
}