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
