package org.dcs.nifi.processors

import java.io.{InputStream, OutputStream}
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.commons.io.IOUtils
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.StreamCallback
import org.dcs.api.processor.{Configuration, MetaData, RelationshipType}
import org.dcs.api.service.RemoteProcessorService
import org.dcs.remote.{RemoteService, ZkRemoteService}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}


trait ClientProcessor extends AbstractProcessor with WriteOutput with IO {

  val logger: Logger = LoggerFactory.getLogger(classOf[ClientProcessor])

  var remoteProcessorService: RemoteProcessorService = _

  var propertyDescriptors: JavaList[PropertyDescriptor] = _
  var relationships: JavaSet[Relationship] = _
  var metaData:MetaData = _
  var configuration: Configuration = _

  def processorClassName(): String

  def remoteService: RemoteService = ZkRemoteService

  def processorService(): RemoteProcessorService = {
    remoteService.loadService[RemoteProcessorService](processorClassName())
  }

  override def init(context: ProcessorInitializationContext) {
    remoteProcessorService = processorService()

    propertyDescriptors = remoteProcessorService.properties.map(ps => PropertyDescriptor(ps)).asJava

    relationships = remoteProcessorService.relationships.map(rs => Relationship(rs))

    metaData = remoteProcessorService.metadata

    configuration = remoteProcessorService.configuration
  }

  override def output(in: Option[InputStream],
                      valueProperties: JavaMap[String, String]): Array[Byte] = in match {
    case None => remoteProcessorService.trigger("".getBytes, valueProperties)
    case Some(input) => remoteProcessorService.trigger(IOUtils.toByteArray(input), valueProperties)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    val valueProperties = context.getProperties.asScala.map(x => (x._1.getName, x._2))
    var flowFile: FlowFile = session.get()
    // FIXME: Currently we only have one client processor type using StreamCallback for write
    //        It may be required to have two more client processor types which allow
    //        InputStreamCallback and OutputStream Callback.
    //        Once the add processor functionality is implemented in the flow api
    //        the type information can be set in the remote processor
    flowFile = writeCallback(flowFile, valueProperties, session)

    val attributes = scala.collection.mutable.Map[String, String]()

    attributes(CoreAttributes.MIME_TYPE.key()) = configuration.outputMimeType

    flowFile = session.putAllAttributes(flowFile, attributes);

    val successRelationship: Option[Relationship] = relationships.find(r => r.getName == RelationshipType.SucessRelationship)
    if (successRelationship.isDefined) {
      session.transfer(flowFile, successRelationship.get);
    }

  }

  override def getRelationships: JavaSet[Relationship] = {
    relationships;
  }

  override def getSupportedPropertyDescriptors: JavaList[PropertyDescriptor] = {
    propertyDescriptors
  }

}