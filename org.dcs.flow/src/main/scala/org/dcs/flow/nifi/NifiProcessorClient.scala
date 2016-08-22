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

  val StateNotStarted = "NOT_STARTED"
  val StateRunning = "RUNNING"
  val StateStopped = "STOPPED"

  val States = Set(StateRunning, StateStopped)

  def processorsPath(processorId: String) =
    "/processors/" + processorId
}

trait NifiProcessorClient extends ProcessorApiService with NifiBaseRestClient {

  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProcessorClient])

  import NifiProcessorClient._

  override def types(userId: String): List[ProcessorType] = {
    val qp = Map(
      ClientIdKey -> userId
    )
    val processorTypes = getAsJson(path = TypesPath,
      queryParams = qp).toObject[ProcessorTypesEntity]
    processorTypes.getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
  }

  def typesSearchTags(str:String, clientToken: String): List[ProcessorType] = {
    types(clientToken).filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
  }

  override def create(name: String, ptype: String, userId: String): ProcessorInstance = {
    val qp = Map(
        ClientIdKey -> userId,
        "name" -> name,
        "type" -> ptype,
        "x" -> "17",
        "y" -> "100")

    val processorEntity = postAsJson(path = processorsPath(userId),
      queryParams = qp,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[ProcessorEntity]

    ProcessorInstance(processorEntity)
  }

  override def instance(processorId: String, userId: String): ProcessorInstance = {
    val processorEntity = getAsJson(processorsPath(processorId)).
      toObject[ProcessorEntity]
    ProcessorInstance(processorEntity)
  }

  override def start(processorId: String, userId: String): ProcessorInstance = {
    start(processorId, instance(processorId, userId).version, userId)
  }

  def start(processorId: String, currentVersion: Long, userId: String): ProcessorInstance = {
    changeState(processorId, currentVersion, StateRunning, userId)
  }

  override def stop(processorId: String, userId: String): ProcessorInstance = {
    stop(processorId, instance(processorId, userId).version, userId)
  }

  def stop(processorId: String, currentVersion: Long, userId: String): ProcessorInstance = {
    changeState(processorId, currentVersion, StateStopped, userId)
  }


  override def remove(processorId: String, userId: String): Boolean = {
    val qp = Map(
      ClientIdKey -> userId
    )
    val processor = deleteAsJson(path = processorsPath(userId) + "/" + processorId,
      queryParams = qp).toObject[ProcessorEntity]

    processor != null
  }

  def changeState(processorId: String, currentVersion: Long, state: String, userId: String): ProcessorInstance = {
    if(!States.contains(state))
      throw new RESTException(ErrorConstants.DCS305.withErrorMessage("State [" + state + "] not recognised"))

    val processorEntity = putAsJson(path = processorsPath(processorId) ,
      obj = ProcessorStateUpdateRequest(processorId, state, currentVersion, userId).toJson
    ).toObject[ProcessorEntity]

    ProcessorInstance(processorEntity)
  }
}