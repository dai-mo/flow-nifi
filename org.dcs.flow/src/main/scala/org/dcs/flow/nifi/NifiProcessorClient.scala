package org.dcs.flow.nifi

import javax.ws.rs.core.MediaType

import org.apache.nifi.web.api.entity.{ProcessorEntity, ProcessorTypesEntity}
import org.dcs.api.service.{ProcessorApiService, ProcessorInstance, ProcessorType}
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.flow.nifi.NifiBaseRestClient._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object NifiProcessorClient  {
  val TypesPath = "/controller/processor-types"
  val ProcessorsPath = "/controller/process-groups/root/processors"
}

trait NifiProcessorClient extends ProcessorApiService with NifiBaseRestClient {

  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProcessorClient])

  import NifiProcessorClient._

  override def types(clientId: String): List[ProcessorType] = {
    val processorTypes = getAsJson(path = TypesPath,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[ProcessorTypesEntity]
    processorTypes.getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
  }

  def typesSearchTags(str:String, clientToken: String): List[ProcessorType] = {
    types(clientToken).filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
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
    processorInstance.setId(processor.getProcessor.getId)
    processorInstance.setStatus(processor.getProcessor.getState)
    processorInstance
  }

  override def start(processorId: String, clientId: String): ProcessorInstance = {

    val processor = putAsJson(path = ProcessorsPath + "/" + processorId,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[ProcessorEntity]

    val processorInstance = new ProcessorInstance
    processorInstance.setId(processor.getProcessor.getId)
    processorInstance.setStatus(processor.getProcessor.getState)
    processorInstance
  }

  override def remove(processorId: String, clientId: String): Boolean = {
    val processor = deleteAsJson(path = ProcessorsPath + "/" + processorId,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[ProcessorEntity]

    processor != null
  }
}