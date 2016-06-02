package org.dcs.flow

import org.dcs.flow.model.{ProcessorInstance, ProcessorType}

trait ProcessorClient {
  
  def types(clientToken: String): List[ProcessorType]

  def create(name: String, ptype: String, clientToken: String): ProcessorInstance

  def start(processorId: String, clientToken: String): ProcessorInstance

  def remove(processorId: String, clientToken: String): Boolean

}