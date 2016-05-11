package org.dcs.nifi.processors

import org.dcs.api.service.ModuleFactoryService
import org.dcs.core.module.flow.FlowModule
import java.util.UUID
import java.nio.charset.StandardCharsets
import java.util.{Map  => JavaMap}

class MockModuleFactoryService(flowModule: FlowModule, response: String) extends ModuleFactoryService {
  
  
  def createFlowModule(className: String): String = {
    UUID.randomUUID().toString()
  }
  def getPropertyDescriptors(moduleUUID: String): JavaMap[String, JavaMap[String,String]] = 
    flowModule.getPropertyDescriptors()
    
  def getRelationships(moduleUUID: String): JavaMap[String, JavaMap[String,String]] = 
    flowModule.getRelationships()
    
  def remove(moduleUUID: String): Boolean =  true
  
  def schedule(moduleUUID: String): Boolean =  true
  
  def shutdown(moduleUUID: String): Boolean = true
  
  def stop(moduleUUID: String): Boolean = true
  
  def trigger(moduleUUID: String,properties: JavaMap[String,String]): Array[Byte] = 
    response.getBytes(StandardCharsets.UTF_8)
    
  def unschedule(moduleUUID: String): Boolean = true
  
}