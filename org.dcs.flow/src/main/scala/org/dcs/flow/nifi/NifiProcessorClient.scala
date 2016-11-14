package org.dcs.flow.nifi

import org.apache.nifi.web.api.entity.{ProcessorEntity, ProcessorTypesEntity}
import org.dcs.api.service.{ProcessorApiService, ProcessorInstance, ProcessorType}
import org.dcs.commons.error.{ErrorConstants, RESTException}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

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

trait NifiProcessorClient extends ProcessorApiService with JerseyRestClient {

  import NifiProcessorClient._

  override def types(userId: String): Future[List[ProcessorType]] =
    getAsJson(path = TypesPath)
      .map { response =>
        response.toObject[ProcessorTypesEntity]
          .getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
      }

  def typesSearchTags(str:String, clientToken: String): Future[List[ProcessorType]] =
    types(clientToken)
      .map { response =>
        response.filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
      }

  override def create(name: String, ptype: String, userId: String): Future[ProcessorInstance] =
    postAsJson(path = processorsPath(userId))
      .map { response =>
        ProcessorInstance(response.toObject[ProcessorEntity])
      }

  override def instance(processorId: String, userId: String): Future[ProcessorInstance] =
    getAsJson(processorsPath(processorId))
      .map { response =>
        ProcessorInstance(response.toObject[ProcessorEntity])
      }

  override def start(processorId: String, userId: String): Future[ProcessorInstance] =
    for {
      instance <- instance(processorId, userId)
      startedInstance <- start(processorId, instance.version, userId)
    } yield startedInstance

  def start(processorId: String, currentVersion: Long, userId: String): Future[ProcessorInstance] = {
    changeState(processorId, currentVersion, StateRunning, userId)
  }

  override def stop(processorId: String, userId: String): Future[ProcessorInstance] =
    for {
      instance <- instance(processorId, userId)
      stoppedInstance <- stop(processorId, instance.version, userId)
    } yield stoppedInstance


  def stop(processorId: String, currentVersion: Long, userId: String): Future[ProcessorInstance] = {
    changeState(processorId, currentVersion, StateStopped, userId)
  }


  override def remove(processorId: String, userId: String): Future[Boolean] =
    deleteAsJson(path = processorsPath(userId) + "/" + processorId)
      .map { response =>
        response.toObject[ProcessorEntity] != null
      }


  def changeState(processorId: String, currentVersion: Long, state: String, userId: String): Future[ProcessorInstance] = {
    if(!States.contains(state))
      throw new RESTException(ErrorConstants.DCS305.withErrorMessage("State [" + state + "] not recognised"))

    putAsJson(path = processorsPath(processorId), body = ProcessorStateUpdateRequest(processorId, state, currentVersion, userId))
      .map { response =>
        ProcessorInstance(response.toObject[ProcessorEntity])
      }
  }
}