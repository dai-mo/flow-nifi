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

import java.util.{List => JavaList, Map => JavaMap}

import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.dcs.api.service.{RemoteProcessorService, StatefulRemoteProcessorService}

import scala.collection.JavaConverters._
/**
  * Created by cmathew on 07/09/16.
  */
trait StatefulClientProcessor extends ClientProcessor {

  var statefulRemoteProcessorService: StatefulRemoteProcessorService = _
  var processorStateId: String = _


  override def processorService(processorClassName: String): Unit = {
    statefulRemoteProcessorService =
      remoteService.loadService[StatefulRemoteProcessorService](processorClassName)
    remoteProcessorService = statefulRemoteProcessorService
  }

  override def init(context: ProcessorInitializationContext): Unit = {
    super.init(context)
  }

  override def initStub(processorClassName: String): Unit = {
    super.initStub(processorClassName)
    processorStateId = statefulRemoteProcessorService.init()
  }


  override def output(in: Option[Array[Byte]],
             valueProperties: JavaMap[String, String]): Array[Array[Byte]] = in match {
    case None => statefulRemoteProcessorService.instanceTrigger(
      processorStateId,
      "".getBytes,
      valueProperties
    )
    case Some(input) => statefulRemoteProcessorService.instanceTrigger(
      processorStateId,
      input,
      valueProperties
    )
  }

  @OnConfigurationRestored
  def onConfigurationRestore(): Unit = {
    Option(statefulRemoteProcessorService).map(_.onInstanceConfigurationRestore(processorStateId))
  }

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit = {
    super.onPropertyModified(descriptor, oldValue, newValue)
    Option(statefulRemoteProcessorService).map(_.onInstancePropertyChanged(processorStateId,
      RemoteProperty(descriptor)))
  }

  @OnAdded
  def onAdd(): Unit = {
    Option(statefulRemoteProcessorService).map(_.onInstanceAdd(processorStateId))
  }

  @OnScheduled
  def onSchedule(processContext: ProcessContext): Unit = {
    Option(statefulRemoteProcessorService).map(_.onInstanceSchedule(processorStateId,
      processContext.getProperties.asScala.map(pdv => (RemoteProperty(pdv._1), pval(pdv._1, pdv._2))).asJava))
  }

  @OnUnscheduled
  def onUnschedule(processContext: ProcessContext) = {
    Option(statefulRemoteProcessorService).map(_.onInstanceUnschedule(processorStateId,
      processContext.getProperties.asScala.map(pdv => (RemoteProperty(pdv._1), pval(pdv._1, pdv._2))).asJava))
  }

  @OnStopped
  def onStop(processContext: ProcessContext) = {
    Option(statefulRemoteProcessorService).map(_.onInstanceStop(processorStateId,
      processContext.getProperties.asScala.map(pdv => (RemoteProperty(pdv._1), pval(pdv._1, pdv._2))).asJava))
  }

  @OnShutdown
  def onShutdown(processContext: ProcessContext) = {
    Option(statefulRemoteProcessorService).map(_.onInstanceShutdown(processorStateId,
      processContext.getProperties.asScala.map(pdv => (RemoteProperty(pdv._1), pval(pdv._1, pdv._2))).asJava))
  }

  @OnRemoved
  def onRemove(processContext: ProcessContext) = {
    Option(statefulRemoteProcessorService).map(_.onInstanceRemove(processorStateId,
      processContext.getProperties.asScala.map(pdv => (RemoteProperty(pdv._1), pval(pdv._1, pdv._2))).asJava))
  }
}
