/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dcs.nifi.processors


import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.dcs.api.processor._
import org.dcs.api.service.RemoteProcessorService
import org.dcs.remote.{RemoteService, ZkRemoteService}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}


trait ClientProcessor extends AbstractProcessor with Write with Read {

  val logger: Logger = LoggerFactory.getLogger(classOf[ClientProcessor])

  var remoteProcessorService: RemoteProcessorService = _
  var processorServiceClassName: String = _
  var propertyDescriptors: JavaList[PropertyDescriptor] = _
  var relationships: JavaSet[Relationship] = new util.HashSet[Relationship]()
  var metaData:MetaData = _
  var configuration: Configuration = _
  var schemaId: Option[String] = None

  var endOfStream = false

  val processorClassPd: PropertyDescriptor = PropertyDescriptor.processorClassPd()
  propertyDescriptors = new util.ArrayList[PropertyDescriptor]()
  propertyDescriptors.add(processorClassPd)

  def remoteService: RemoteService = ZkRemoteService

  def processorService(processorServiceClassName: String): Unit = {
    this.processorServiceClassName = processorServiceClassName
    remoteProcessorService = remoteService.loadService[RemoteProcessorService](processorServiceClassName)
  }

  override def init(context: ProcessorInitializationContext) {

  }

  protected def initStub(processorServiceClassName: String): Unit = {
    processorService(processorServiceClassName)

    propertyDescriptors = remoteProcessorService.properties.map(ps => PropertyDescriptor(ps)).asJava

    relationships = remoteProcessorService.relationships.map(rs => Relationship(rs))

    metaData = remoteProcessorService.metadata

    configuration = remoteProcessorService.configuration

    schemaId = Option(remoteProcessorService.schemaId)
  }

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit = {
    if(descriptor.getDisplayName == CoreProperties.ProcessorClassKey) initStub(newValue)

    super.onPropertyModified(descriptor, oldValue, newValue)
  }

  def output(in: Option[Array[Byte]],
             valueProperties: JavaMap[String, String]): Array[Array[Byte]] = in match {
    case None => remoteProcessorService.trigger("".getBytes, valueProperties)
    case Some(input) => remoteProcessorService.trigger(input, valueProperties)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession) {
    // FIXME : Implement batching by using session.get(n) to get a max of n flow files from the queue
    
    if (endOfStream) {
      context.`yield`()
    } else {
      val in: AtomicReference[Array[Byte]] = new AtomicReference()

      val valueProperties = context.getProperties.asScala.map(x => (x._1.getName, pval(x._1, x._2)))
      val flowFile: FlowFile = session.get()


      if (canRead && flowFile != null)
        readCallback(flowFile, session, in)

      val out = output(Option(in.get()), valueProperties)

      if (out == null || out.isEmpty) {
        context.`yield`()
        endOfStream = true
      } else {
        if (canWrite) {
          if (out.length == 2) {
            val ff = if (flowFile == null) session.create() else flowFile
            val relationship = new String(out(0))
            route(writeCallback(ff, session,out(1)), session, relationship)
          } else {
            out.grouped(2).foreach { resGrp =>
              val ff = if (flowFile == null) session.create() else session.create(flowFile)
              val relationship = new String(resGrp(0))
              route(writeCallback(ff, session, resGrp(1)), session, relationship)
            }
          }
        }
      }
    }
  }

  protected def pval(pd: PropertyDescriptor, value: String) =
    if(value == null) pd.getDefaultValue else value

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
    rel.foreach(relo => {
      val attributes: util.Map[String, String] = new util.HashMap()
      attributes.put(Attributes.RelationshipAttributeKey, relo.getName)
      attributes.put(Attributes.ComponentTypeAttributeKey, processorServiceClassName)
      val ff = session.putAllAttributes(flowFile, attributes)
      session.transfer(ff, relo)
    })
    if(rel.isEmpty) logger.warn("Ignore transfer of flowfile with id " + flowFile.getId + " to relationship " + relationship + ", as it is not registered")
  }

  def canRead: Boolean

  def canWrite: Boolean

}