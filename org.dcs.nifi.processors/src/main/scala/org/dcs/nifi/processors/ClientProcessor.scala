package org.dcs.nifi.processors

import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor._
import org.dcs.api.processor.{Configuration, MetaData, RelationshipType, RemoteProcessor}
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
  var schemaId: Option[String] = None

  var endOfStream = false

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

    schemaId = Option(remoteProcessorService.schemaId)
  }


  def output(in: Option[Array[Byte]],
             valueProperties: JavaMap[String, String]): Array[Array[Byte]] = in match {
    case None => remoteProcessorService.trigger("".getBytes, valueProperties)
    case Some(input) => remoteProcessorService.trigger(input, valueProperties)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    if (endOfStream) {
      context.`yield`()
    } else {
      val in: AtomicReference[Array[Byte]] = new AtomicReference()

      val valueProperties = context.getProperties.asScala.map(x => (x._1.getName, x._2))
      val flowFile: FlowFile = session.get()

      if (canRead)
        readCallback(flowFile, session, in)

      val out = output(Option(in.get()),
        updateProperties(valueProperties, flowFile))

      if (out == null || out.isEmpty) {
        context.`yield`()
        endOfStream = true
      } else {

        if (canWrite) {
          if (out.length == 3){
            val relationship = new String(out(0))
            val schemaId = Option(new String(out(1)))
            route(
              writeCallback(
                updateFlowFileAttributes(flowFile,
                  session,
                  schemaId,
                  relationship),
                session,
                out(2)
              ),
              session,
              relationship
            )
          } else {
            out.grouped(3).foreach { resGrp =>
              val newFlowFile = if (flowFile == null) session.create() else session.create(flowFile)
              val relationship = new String(resGrp(0))
              val schemaId = Option(new String(resGrp(1)))
              route(
                writeCallback(
                  updateFlowFileAttributes(newFlowFile,
                    session,
                    schemaId,
                    relationship),
                  session,
                  resGrp(2)),
                session,
                relationship
              )
            }

          }
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
            session: ProcessSession,
            relationship: String) = {
    val rel: Option[Relationship] =
      relationships.asScala.find(r => r.getName == relationship)
    if (rel.isDefined) {
      session.transfer(flowFile, rel.get)
    }
  }

  def updateFlowFileAttributes(flowFile: FlowFile,
                               session: ProcessSession,
                               schemaId: Option[String],
                               relationship: String): FlowFile = {
    val attributes = mutable.Map[String, String]()
    attributes(CoreAttributes.MIME_TYPE.key()) = configuration.outputMimeType
    var updatedFlowFile = flowFile
    if(schemaId.isDefined && relationship != RelationshipType.FailureRelationship)
      attributes(RemoteProcessor.SchemaIdKey) = schemaId.get
    else
      updatedFlowFile = session.removeAttribute(updatedFlowFile, RemoteProcessor.SchemaIdKey)
    session.putAllAttributes(updatedFlowFile, attributes.asJava)
  }

  def updateProperties(properties: mutable.Map[String, String],
                       flowFile: FlowFile): mutable.Map[String, String] = {
    if(flowFile != null) {
      val schemaId = flowFile.getAttribute(RemoteProcessor.SchemaIdKey)
      if (schemaId != null)
        properties.put(RemoteProcessor.SchemaIdKey, schemaId)
    }
    properties
  }

  def canRead: Boolean

  def canWrite: Boolean

}