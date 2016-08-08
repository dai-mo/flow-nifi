package org.dcs.flow.nifi

import javax.ws.rs.core.MediaType

import org.apache.nifi.web.api.entity.{ProcessorEntity, ProcessorTypesEntity}
import org.dcs.api.error.{ErrorConstants, RESTException}
import org.dcs.api.service.{ProcessorApiService, ProcessorInstance, ProcessorType}
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.flow.nifi.NifiBaseRestClient._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class NifiProcessorApi extends NifiProcessorClient with NifiApiConfig

object NifiProcessorClient  {
  val TypesPath = "/controller/processor-types"

  val StateRunning = "RUNNING"
  val StateStopped = "STOPPED"

  val States = Set(StateRunning, StateStopped)

  def processorsPath(processGroupId: String) =
    "/controller/process-groups/" + processGroupId + "/processors"
}

trait NifiProcessorClient extends ProcessorApiService with NifiBaseRestClient {

  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProcessorClient])

  import NifiProcessorClient._

  override def types(userID: String): List[ProcessorType] = {
    val processorTypes = getAsJson(path = TypesPath,
      queryParams = (ClientIdKey -> userID) :: Nil).toObject[ProcessorTypesEntity]
    processorTypes.getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
  }

  def typesSearchTags(str:String, clientToken: String): List[ProcessorType] = {
    types(clientToken).filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
  }

  override def create(name: String, ptype: String, userId: String): ProcessorInstance = {

    val processor = postAsJson(path = processorsPath(userId),
      queryParams = (ClientIdKey -> userId) :: List(("name", name),
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

  override def start(processorId: String, processGroupId: String): ProcessorInstance = {
    changeState(processorId, StateRunning, processGroupId)
  }


  override def stop(processorId: String, processGroupId: String): ProcessorInstance = {
    changeState(processorId, StateStopped, processGroupId)
  }


  override def remove(processorId: String, userId: String): Boolean = {
    val processor = deleteAsJson(path = processorsPath(userId) + "/" + processorId,
      queryParams = (ClientIdKey -> userId) :: Nil).toObject[ProcessorEntity]

    processor != null
  }

  def changeState(processorId: String, state: String, processGroupId: String): ProcessorInstance = {
    if(!States.contains(state))
      throw new RESTException(ErrorConstants.DCS305.withErrorMessage("State [" + state + "] not recognised"))

    val qp = List(
      "state" -> state,
      ClientIdKey -> processGroupId
    )
    val processor = putAsJson(path = processorsPath(processGroupId) + "/" + processorId,
      queryParams = qp,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[ProcessorEntity]

    val processorInstance = new ProcessorInstance
    processorInstance.setId(processor.getProcessor.getId)
    processorInstance.setStatus(processor.getProcessor.getState)
    processorInstance
  }
}