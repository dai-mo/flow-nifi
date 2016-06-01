package org.dcs.flow

import org.dcs.flow.model.{ProcessorInstance, ProcessorType}

trait ProcessorClient {
  
  def types(): List[ProcessorType]

  def create(name: String, ptype: String): ProcessorInstance

  def start(processorId: String): ProcessorInstance

}