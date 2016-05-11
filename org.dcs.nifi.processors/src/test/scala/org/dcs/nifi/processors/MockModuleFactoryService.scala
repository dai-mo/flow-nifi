package org.dcs.nifi.processors

import org.dcs.api.service.ModuleFactoryService
import org.dcs.core.module.flow.FlowModule
import java.util.UUID
import java.nio.charset.StandardCharsets

class MockModuleFactoryService(flowModule: FlowModule, response: String) extends ModuleFactoryService {
  
  
  def createFlowModule(className: String): String = {
    UUID.randomUUID().toString()
  }
  def getPropertyDescriptors(moduleUUID: String): Map[String,Map[String,String]] = 
    flowModule.getPropertyDescriptors()
    
  def getRelationships(moduleUUID: String): Map[String,Map[String,String]] = 
    flowModule.getRelationships()
    
  def remove(moduleUUID: String): Boolean =  true
  
  def schedule(moduleUUID: String): Boolean =  true
  
  def shutdown(moduleUUID: String): Boolean = true
  
  def stop(moduleUUID: String): Boolean = true
  
  def trigger(moduleUUID: String,properties: Map[String,String]): Array[Byte] = 
    response.getBytes(StandardCharsets.UTF_8)
    
  def unschedule(moduleUUID: String): Boolean = true
  
}