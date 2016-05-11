package org.dcs.nifi.processors

import org.dcs.nifi.processors.MockZookeeperServiceTracker._
import org.dcs.remote.ServiceTracker
import scala.reflect.ClassTag

object MockZookeeperServiceTracker {
  
  var mockServiceMap = collection.mutable.Map[String, AnyRef]()
      
  def addService(serviceName:String, service: AnyRef) {
    mockServiceMap(serviceName) = service
  }
}

trait MockZookeeperServiceTracker extends ServiceTracker {

  
  def start = {}
  def service[T](implicit tag: ClassTag[T]): Option[T] = {
    val serviceName = tag.runtimeClass.getName
    Some(mockServiceMap(serviceName).asInstanceOf[T])    
  }
  
  def service[T](serviceImplName: String)(implicit tag: ClassTag[T]): Option[T] = {
     Some(mockServiceMap(serviceImplName).asInstanceOf[T]) 
  }
  def close = {}
  
}