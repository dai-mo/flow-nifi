package org.dcs.nifi.processors

import org.dcs.api.processor.RemoteProcessor
import org.dcs.nifi.processors.MockZookeeperServiceTracker._
import org.dcs.remote.ServiceTracker

import scala.reflect.ClassTag

object MockZookeeperServiceTracker {
  
  var mockProcessorMap = collection.mutable.Map[String, RemoteProcessor]()
      
  def addProcessor(processorName:String, processor: RemoteProcessor) {
    mockProcessorMap(processorName) = processor
  }
}

trait MockZookeeperServiceTracker extends ServiceTracker {

  
  def start = {}
  def service[T](implicit tag: ClassTag[T]): Option[T] = {
    val serviceName = tag.runtimeClass.getName
    Some(mockProcessorMap(serviceName).asInstanceOf[T])
  }
  
  def service[T](serviceImplName: String)(implicit tag: ClassTag[T]): Option[T] = {
     Some(mockProcessorMap(serviceImplName).asInstanceOf[T])
  }
  def close = {}
  
}