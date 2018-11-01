/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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