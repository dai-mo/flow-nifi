package org.dcs.nifi.processors

import java.io.{InputStream, OutputStream}
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.StreamCallback
import org.dcs.api.processor.{Configuration, RelationshipType, RemoteProcessor}
import org.dcs.remote.{RemoteService, ZkRemoteService}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}



abstract class ClientProcessor extends AbstractProcessor {

  val logger: Logger = LoggerFactory.getLogger(classOf[ClientProcessor])

  var remoteProcessor: RemoteProcessor = _
  var remoteService: RemoteService = _

  var propertyDescriptors: JavaList[PropertyDescriptor] = Nil
  var relationships: JavaSet[Relationship] = _
  var configuration: Configuration = _


  def processorClassName(): String

  override def init(context: ProcessorInitializationContext) {

    if(remoteService == null) remoteService = ZkRemoteService

    remoteProcessor = remoteService.loadService[RemoteProcessor](processorClassName())

    propertyDescriptors = remoteProcessor.properties.map(ps => PropertyDescriptor(ps)).asJava

    relationships = remoteProcessor.relationships.map(rs => Relationship(rs))

    configuration = remoteProcessor.configuration

  }

  @OnScheduled
  def onScheduled(context: ProcessContext) {

  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    val valueProperties = context.getProperties().asScala.map(x => (x._1.getName, x._2))
    var flowFile: FlowFile = session.get()
    // FIXME: Currently we only have one client processor type using StreamCallback for write
    //        It may be required to have two more client processor types which allow
    //        InputStreamCallback and OutputStream Callback.
    //        Once the add processor functionality is implemented in the flow api
    //        the type information can be set in the remote processor
    flowFile = session.write(flowFile, new StreamCallback() {
      override def process(in: InputStream, out: OutputStream) {
        out.write(remoteProcessor.trigger(IOUtils.toByteArray(in), valueProperties));
      }
    })

    val attributes = scala.collection.mutable.Map[String, String]()

    attributes(CoreAttributes.MIME_TYPE.key()) = "application/json"
    attributes(CoreAttributes.FILENAME.key()) = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json"
    flowFile = session.putAllAttributes(flowFile, attributes);

    val successRelationship: Option[Relationship] = relationships.find(r => r.getName == RelationshipType.SucessRelationship)
    if (successRelationship != None) {
      session.transfer(flowFile, successRelationship.get);
    }

  }

  override def getRelationships: JavaSet[Relationship] = {
    relationships;
  }

  override def getSupportedPropertyDescriptors: JavaList[PropertyDescriptor] = {
    propertyDescriptors;
  }

}