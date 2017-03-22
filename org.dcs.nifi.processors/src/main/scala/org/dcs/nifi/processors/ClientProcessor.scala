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
import org.dcs.api.processor.Attributes


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

      val out = output(Option(in.get()), valueProperties)

      if (out == null || out.isEmpty) {
        context.`yield`()
        endOfStream = true
      } else {
        if (canWrite) {
          if (out.length == 2){
            val relationship = new String(out(0))
            route(writeCallback(flowFile, session,out(1)), session, relationship)
          } else {
            out.grouped(2).foreach { resGrp =>
              val newFlowFile = if (flowFile == null) session.create() else session.create(flowFile)
              val relationship = new String(resGrp(0))
              route(writeCallback(newFlowFile, session, resGrp(1)), session,relationship)
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
            relationship: String): Unit = {
    val rel: Option[Relationship] =
      relationships.asScala.find(r => r.getName == relationship)
    rel.foreach(rel => {
      val ff = session.putAttribute(flowFile, Attributes.RelationshipAttributeKey,rel.getName)
      session.transfer(ff, rel)
    })
    if(rel.isEmpty) logger.warn("Ignore transfer of flowfile with id " + flowFile.getId + " to relationship " + relationship + ", as it is not registered")
  }

  def canRead: Boolean

  def canWrite: Boolean

}