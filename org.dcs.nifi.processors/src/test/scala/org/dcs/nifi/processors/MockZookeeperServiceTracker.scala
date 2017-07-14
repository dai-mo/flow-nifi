package org.dcs.nifi.processors

import org.dcs.api.service.RemoteProcessorService
import org.dcs.nifi.processors.MockZookeeperServiceTracker._
import org.dcs.remote.ServiceTracker

import scala.reflect.ClassTag

object MockZookeeperServiceTracker {
  
  var mockProcessorMap = collection.mutable.Map[String, RemoteProcessorService]()
      
  def addProcessor(processorName:String, processorService: RemoteProcessorService) {
    mockProcessorMap(processorName) = processorService
  }
}

trait MockZookeeperServiceTracker extends ServiceTracker {

  def init = {}
  def start = {}
  def service[T](implicit tag: ClassTag[T]): Option[T] = {
    val serviceName = tag.runtimeClass.getName
    Some(mockProcessorMap(serviceName).asInstanceOf[T])
  }

  def filterServiceByProperty(property: String,regex: String): List[org.dcs.api.service.ProcessorServiceDefinition] = Nil

  def services(): List[org.dcs.api.service.ProcessorServiceDefinition] = Nil


  def service[T](serviceImplName: String)(implicit tag: ClassTag[T]): Option[T] = {
     Some(mockProcessorMap(serviceImplName).asInstanceOf[T])
  }
  def close = {}
  
}