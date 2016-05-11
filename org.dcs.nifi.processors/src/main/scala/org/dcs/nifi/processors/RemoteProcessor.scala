package org.dcs.nifi.processors

import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessorInitializationContext
import org.dcs.api.service.ModuleFactoryService
import org.dcs.remote.ZkRemoteService
import org.dcs.remote.RemoteService
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.processor.ProcessSession
import java.util.Properties
import collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.OutputStreamCallback
import java.io.OutputStream
import org.apache.nifi.flowfile.attributes.CoreAttributes
import java.util.{Map  => JavaMap}
import java.util.{Set  => JavaSet}
import java.util.{List => JavaList}
import scala.collection.mutable.{ Map => MutableMap }
import org.slf4j.LoggerFactory
import org.slf4j.Logger

abstract class RemoteProcessor extends AbstractProcessor {

  val logger: Logger = LoggerFactory.getLogger(classOf[RemoteProcessor])

  var flowModuleId: String = _
  var propertyDescriptors: JavaList[PropertyDescriptor] = Nil
  var relationships: JavaSet[Relationship] = _
  var moduleFactoryService: Option[ModuleFactoryService] = None
  
  var remoteService: RemoteService = _

  def flowModuleClassName(): String

  override def init(context: ProcessorInitializationContext) {

    if(remoteService == null) remoteService = ZkRemoteService 
    
    moduleFactoryService = remoteService.loadService[ModuleFactoryService]

    if (moduleFactoryService != None) {

      flowModuleId = moduleFactoryService.get.createFlowModule(flowModuleClassName())

      logger.info("Created flow module " + flowModuleClassName() + " with id " + flowModuleId)

      val propDescMap: JavaMap[String, JavaMap[String, String]] = moduleFactoryService.get.getPropertyDescriptors(flowModuleId)
      propertyDescriptors = ProcessorUtils.generatePropertyDescriptors(propDescMap)

      val relationshipsMap: JavaMap[String, JavaMap[String, String]] = moduleFactoryService.get.getRelationships(flowModuleId)
      relationships = ProcessorUtils.generateRelationships(relationshipsMap)

    } else {
      throw new IllegalStateException("Remote service module of type " + flowModuleClassName + " with module id " + flowModuleId + " not available")
    }

  }

  @OnScheduled
  def onScheduled(context: ProcessContext) {

  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    val valueProperties = ProcessorUtils.valueProperties(context.getProperties())
    var flowFile: FlowFile = session.create();
    flowFile = session.write(flowFile, new OutputStreamCallback() {
      override def process(out: OutputStream) {
        out.write(moduleFactoryService.get.trigger(flowModuleId, valueProperties));
      }
    })

    val attributes = scala.collection.mutable.Map[String, String]()
    attributes(CoreAttributes.MIME_TYPE.key()) = "application/json"
    attributes(CoreAttributes.FILENAME.key()) = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json"
    flowFile = session.putAllAttributes(flowFile, attributes);

    val successRelationship: Option[Relationship] = ProcessorUtils.successRelationship(relationships);
    if (successRelationship != None) {
      session.transfer(flowFile, successRelationship.get);
    }

  }

  override def getRelationships(): JavaSet[Relationship] = {
    return relationships;
  }

  override def getSupportedPropertyDescriptors(): JavaList[PropertyDescriptor] = {
    return propertyDescriptors;
  }

}