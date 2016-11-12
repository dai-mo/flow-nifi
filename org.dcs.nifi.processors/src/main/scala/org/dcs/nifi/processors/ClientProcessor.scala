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


  def response(out: JavaList[Either[Array[Byte], Array[Byte]]]): JavaList[Array[Byte]] = {
    out.asScala.map { result =>
      if (result.isLeft)
        result.left.get
      else
        result.right.get
    }
  }

  override def output(in: Option[InputStream],
                      valueProperties: JavaMap[String, String]): JavaList[Array[Byte]] = in match {
    case None => response(remoteProcessorService.trigger(
      "".getBytes,
      valueProperties)
    )
    case Some(input) => response(remoteProcessorService.trigger(
      IOUtils.toByteArray(input),
      valueProperties)
    )
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    val valueProperties = context.getProperties.asScala.map(x => (x._1.getName, x._2))
    var flowFile: FlowFile = session.get()
    flowFile = writeCallback(flowFile, valueProperties, session, configuration, relationships)
  }

  override def getRelationships: JavaSet[Relationship] = {
    relationships
  }

  override def getSupportedPropertyDescriptors: JavaList[PropertyDescriptor] = {
    propertyDescriptors
  }

}