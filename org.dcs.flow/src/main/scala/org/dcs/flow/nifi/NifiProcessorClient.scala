package org.dcs.flow.nifi

import javax.ws.rs.core.{Form, MediaType}

import org.apache.nifi.web.api.entity.ProcessorTypesEntity
import org.apache.nifi.web.api.entity.ProcessorEntity
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.commons.JsonUtil
import org.dcs.flow.ProcessorClient
import org.dcs.flow.model.{ProcessorInstance, ProcessorType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import NifiBaseRestClient._

object NifiProcessorClient  {
  val TypesPath = "/controller/processor-types"
  val ProcessorsPath = "/controller/process-groups/root/processors"
}

trait NifiProcessorClient extends ProcessorClient with NifiBaseRestClient {

  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProcessorClient])

  import NifiProcessorClient._

  override def types(clientId: String): List[ProcessorType] = {
    val processorTypes = getAsJson(path = TypesPath,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[ProcessorTypesEntity]
    processorTypes.getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
  }

  override def create(name: String, ptype: String, clientId: String): ProcessorInstance = {

    val processor = postAsJson(path = ProcessorsPath,
      queryParams = (ClientIdKey -> clientId) :: List(("name", name),
        ("type" -> ptype),
        ("x" -> "17"),
        ("y" -> "100")),
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[ProcessorEntity]

    val processorInstance = new ProcessorInstance
    processorInstance.id = processor.getProcessor.getId
    processorInstance.status = processor.getProcessor.getState
    processorInstance
  }

  override def start(processorId: String, clientId: String): ProcessorInstance = {

    val processor = putAsJson(path = ProcessorsPath + "/" + processorId,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[ProcessorEntity]

    val processorInstance = new ProcessorInstance
    processorInstance.id = processor.getProcessor.getId
    processorInstance.status = processor.getProcessor.getState
    processorInstance
  }

  override def remove(processorId: String, clientId: String): Boolean = {
    val processor = deleteAsJson(path = ProcessorsPath + "/" + processorId,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[ProcessorEntity]

    processor != null
  }
}