package org.dcs.nifi.processors

import java.io.InputStream
import java.util.{Map => JavaMap}

import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.lifecycle._
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.{ProcessContext, ProcessorInitializationContext}
import org.dcs.api.service.{RemoteProcessorService, StatefulRemoteProcessorService}

import scala.collection.JavaConverters._
/**
  * Created by cmathew on 07/09/16.
  */
abstract class StatefulClientProcessor extends ClientProcessor {

  var statefulRemoteProcessorService: StatefulRemoteProcessorService = _
  var processorStateId: String = _


  override def processorService(): RemoteProcessorService = {
    statefulRemoteProcessorService
  }

  override def init(context: ProcessorInitializationContext): Unit = {
    // FIXME: This remote service load does not work due to a null type in the
    //        StatefulRemoteProcessorService declaration
    //        Maybe due to the AnyRef return object of the execute method
    statefulRemoteProcessorService =
      remoteService.loadService[StatefulRemoteProcessorService](processorClassName())
    super.init(context)
    processorStateId = statefulRemoteProcessorService.init()


  }

  override def output(in: Option[InputStream],
                      valueProperties: JavaMap[String, String]): Array[Byte] = in match {
    case None => statefulRemoteProcessorService.instanceTrigger(processorStateId, "".getBytes, valueProperties)
    case Some(input) => statefulRemoteProcessorService.instanceTrigger(processorStateId, IOUtils.toByteArray(input), valueProperties)
  }

  @OnConfigurationRestored
  def onConfigurationRestore(): Unit = {
    statefulRemoteProcessorService.onInstanceConfigurationRestore(processorStateId)
  }

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit = {
    statefulRemoteProcessorService.onInstancePropertyChanged(processorStateId,
      RemoteProperty(descriptor))
  }

  @OnAdded
  def onAdd(): Unit = {
    statefulRemoteProcessorService.onInstanceAdd(processorStateId)
  }

  @OnScheduled
  def onSchedule(processContext: ProcessContext): Unit = {
    statefulRemoteProcessorService.onInstanceSchedule(processorStateId,
      processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnUnscheduled
  def onUnschedule(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onInstanceUnschedule(processorStateId,
      processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnStopped
  def onStop(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onInstanceStop(processorStateId,
      processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnShutdown
  def onShutdown(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onInstanceShutdown(processorStateId,
      processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnRemoved
  def onRemove(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onInstanceRemove(processorStateId,
      processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }
}
