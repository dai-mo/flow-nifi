package org.dcs.flow.nifi

import org.apache.nifi.web.api.entity.{ProcessorEntity, ProcessorTypesEntity}
import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service.{ProcessorApiService, ProcessorInstance, ProcessorServiceDefinition, ProcessorType}
import org.dcs.commons.error.{ErrorConstants, RESTException}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.nifi.internal.ProcessGroupHelper

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

  def processorsProcessGroupPath(processGroup: String): String =
    "/process-groups/" + processGroup + "/processors"

  def processorsPath(processorId: String): String =
    "/processors/" + processorId

  def processorDescriptorsPath(processorId: String): String =
    "/processors/" + processorId + "/descriptors"
}

trait NifiProcessorClient extends ProcessorApiService with JerseyRestClient {

  import NifiProcessorClient._

  override def types(): Future[List[ProcessorType]] =
    getAsJson(path = TypesPath)
      .map { response =>
        response.toObject[ProcessorTypesEntity]
          .getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
      }

  def typesSearchTags(str:String): Future[List[ProcessorType]] =
    types()
      .map { response =>
        response.filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
      }

  def createBaseProcessor(processorServiceDefinition: ProcessorServiceDefinition,
                          processGroupId: String,
                          clientId: String): Future[ProcessorEntity] =
    postAsJson(path = processorsProcessGroupPath(processGroupId),
      body = FlowProcessorRequest(processorServiceDefinition, clientId),
      queryParams = Revision.params(clientId))
      .map { response =>
        response.toObject[ProcessorEntity]
      }

  def updateProcessorClass(processorServiceClassName: String, processorEntity: ProcessorEntity): Future[ProcessorEntity] =
    putAsJson(path = processorsPath(processorEntity.getId),
      body = FlowProcessorUpdateRequest(Map(RemoteProcessor.RemoteProcessorClassKey -> processorServiceClassName), processorEntity))
      .map { response =>
        response.toObject[ProcessorEntity]
      }

  def autoTerminateAllRelationships(processorEntity: ProcessorEntity): Future[ProcessorEntity] =
    putAsJson(path = processorsPath(processorEntity.getId),
      body = FlowProcessorUpdateRequest(processorEntity.getComponent.getRelationships.asScala.map(_.getName).toSet, processorEntity))
      .map { response =>
        response.toObject[ProcessorEntity]
      }

  override def create(processorServiceDefinition: ProcessorServiceDefinition,
                      processGroupId: String,
                      clientId: String): Future[ProcessorInstance] =
    for {
      baseProcessor <- createBaseProcessor(processorServiceDefinition, processGroupId, clientId)
      stubProcessor <- updateProcessorClass(processorServiceDefinition.processorServiceClassName, baseProcessor)
      finalisedProcessor <- autoTerminateAllRelationships(stubProcessor)
    } yield ProcessorInstance(finalisedProcessor)

  override def instance(processorId: String): Future[ProcessorInstance] =
    getAsJson(processorsPath(processorId))
      .map { response =>
        ProcessorInstance(response.toObject[ProcessorEntity])
      }

  override def start(processorId: String, userId: String): Future[ProcessorInstance] =
    for {
      instance <- instance(processorId)
      startedInstance <- start(processorId, instance.version, userId)
    } yield startedInstance

  def start(processorId: String, currentVersion: Long, userId: String): Future[ProcessorInstance] = {
    changeState(processorId, currentVersion, StateRunning, userId)
  }

  override def stop(processorId: String, userId: String): Future[ProcessorInstance] =
    for {
      instance <- instance(processorId)
      stoppedInstance <- stop(processorId, instance.version, userId)
    } yield stoppedInstance


  def stop(processorId: String, currentVersion: Long, userId: String): Future[ProcessorInstance] = {
    changeState(processorId, currentVersion, StateStopped, userId)
  }


  override def remove(processorId: String, version: Long, clientId: String): Future[Boolean] =
    deleteAsJson(path = processorsPath(processorId),
      queryParams = Revision.params(version, clientId))
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