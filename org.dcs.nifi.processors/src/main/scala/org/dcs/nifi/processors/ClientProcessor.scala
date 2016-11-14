package org.dcs.nifi.processors

import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor._
import org.dcs.api.processor.{Configuration, MetaData, RelationshipType}
import org.dcs.api.service.RemoteProcessorService
import org.dcs.remote.{RemoteService, ZkRemoteService}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{Map => MutableMap}


trait ClientProcessor extends AbstractProcessor with Write with Read {

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

  def output(in: Option[Array[Byte]],
             valueProperties: JavaMap[String, String]): Array[Array[Byte]] = in match {
    case None => remoteProcessorService.trigger("".getBytes, valueProperties)
    case Some(input) => remoteProcessorService.trigger(input, valueProperties)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    val in: AtomicReference[Array[Byte]] = new AtomicReference()

    val valueProperties = context.getProperties.asScala.map(x => (x._1.getName, x._2))
    val flowFile: FlowFile = session.get()

    if(canRead)
      readCallback(flowFile, session, in)

    val out = output(Option(in.get()), valueProperties)

    if(canWrite) {
      if(out.length == 1) {
        route(writeCallback(update(flowFile, session), session, out(0)), session)
      } else {
        out.foreach { response =>
          val newFlowFile = session.create(flowFile)
          route(writeCallback(newFlowFile, session, response), session)
        }
      }
    }
  }

  override def getRelationships: JavaSet[Relationship] = {
    relationships
  }

  override def getSupportedPropertyDescriptors: JavaList[PropertyDescriptor] = {
    propertyDescriptors
  }

  def route(flowFile: FlowFile,
            session: ProcessSession) = {
      val successRelationship: Option[Relationship] =
        relationships.asScala.find(r => r.getName == RelationshipType.SucessRelationship)
      if (successRelationship.isDefined) {
        session.transfer(flowFile, successRelationship.get)
      }
    }

  def update(flowFile: FlowFile,
             session: ProcessSession): FlowFile = {
    val attributes = mutable.Map[String, String]()
    attributes(CoreAttributes.MIME_TYPE.key()) = configuration.outputMimeType
    session.putAllAttributes(flowFile, attributes.asJava)
  }

  def canRead: Boolean

  def canWrite: Boolean

}