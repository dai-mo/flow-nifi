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
    case None => statefulRemoteProcessorService.trigger(processorStateId, "".getBytes(), valueProperties)
    case Some(in) => statefulRemoteProcessorService.trigger(processorStateId, IOUtils.toByteArray(in), valueProperties)
  }

  @OnConfigurationRestored
  def onConfigurationRestore(): Unit = {
    statefulRemoteProcessorService.onConfigurationRestore(processorStateId)
  }

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit = {
    statefulRemoteProcessorService.onPropertyChanged(processorStateId, RemoteProperty(descriptor))
  }

  @OnAdded
  def onAdd(): Unit = {
    statefulRemoteProcessorService.onAdd(processorStateId)
  }

  @OnScheduled
  def onSchedule(processContext: ProcessContext): Unit = {
    statefulRemoteProcessorService.onSchedule(processorStateId, processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnUnscheduled
  def onUnschedule(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onUnschedule(processorStateId, processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnStopped
  def onStop(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onStop(processorStateId, processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnShutdown
  def onShutdown(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onShutdown(processorStateId, processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }

  @OnRemoved
  def onRemove(processContext: ProcessContext) = {
    statefulRemoteProcessorService.onRemove(processorStateId, processContext.getProperties.asScala.keys.map(pd => RemoteProperty(pd)).toList.asJava)
  }
}
