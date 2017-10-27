package org.dcs.flow.nifi

import org.apache.nifi.web.api.entity.{ConnectionEntity, DropRequestEntity}
import org.dcs.api.processor.{CoreProperties, ExternalProcessorProperties}
import org.dcs.api.service._
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.{FlowApi, FlowGraph, FlowGraphTraversal}
import org.glassfish.jersey.filter.LoggingFilter

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

/**
  * Created by cmathew on 18.04.17.
  */

class NifiConnectionApi extends NifiConnectionClient with NifiApiConfig

object NifiConnectionClient {

  val ioPortApi = new NifiIOPortApi

  val processorApi = new NifiProcessorApi

  val MaxDropRequestCheckCount = 100

  def connectionsProcessGroupPath(processGroupId: String): String =
    "/process-groups/" + processGroupId + "/connections"

  def connectionsPath(connectionId: String): String =
    "/connections/" + connectionId

  def connectionQueuePath(connectionId: String): String =
    "/flowfile-queues/" + connectionId + "/drop-requests"

  def connectionQueueDropRequestPath(connectionId: String, dropRequestId: String): String =
    "/flowfile-queues/" + connectionId + "/drop-requests/" + dropRequestId
}

trait NifiConnectionClient extends ConnectionApiService with JerseyRestClient {

  import NifiConnectionClient._

  override def list(processGroupId: String): Future[List[Connection]] = {
    getAsJson(path = connectionsProcessGroupPath(processGroupId))
      .map { response =>
        response.toObject[List[ConnectionEntity]].map(ce => ConnectionAdapter(ce))
      }
  }

  override def find(connectionId: String, clientId: String): Future[Connection] = {
    getAsJson(path = connectionsPath(connectionId))
      .map { response =>
        ConnectionAdapter(response.toObject[ConnectionEntity])
      }
  }

  override def create(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {
    (connectionConfig.source.componentType, connectionConfig.destination.componentType) match {
      case (FlowComponent.ProcessorType, FlowComponent.ProcessorType) =>
        createProcessorConnection(connectionConfig, clientId)
      case (FlowComponent.ProcessorType, FlowComponent.ExternalProcessorType) =>
        createConnectionToExternalProcessor(connectionConfig, clientId)
      case (FlowComponent.ExternalProcessorType, FlowComponent.ProcessorType) =>
        createConnectionFromExternalProcessor(connectionConfig, clientId)
      case ( _ , FlowComponent.InputPortIngestionType) =>
        createConnectionForInputPortIngestionProcessor(connectionConfig, clientId)
      case (FlowComponent.InputPortType, FlowComponent.InputPortType) |
           (FlowComponent.OutputPortType, FlowComponent.OutputPortType) |
           (FlowComponent.InputPortType, FlowComponent.ProcessorType) |
           (FlowComponent.ProcessorType, FlowComponent.OutputPortType) =>
        createStdConnection(connectionConfig, clientId)
      case _ => throw new IllegalArgumentException("Cannot connect source of type " + connectionConfig.source.componentType +
        " to destination of type " + connectionConfig.destination.componentType)
    }
  }

  def propagateSchema(startProcessorId: String, flowInstanceId: String, clientId: String): Future[List[ProcessorInstance]] = {
    processorApi.instance(startProcessorId)
      .flatMap(p => FlowApi.instance(flowInstanceId, clientId)
        .map(fi => FlowGraph.executeBreadthFirstFromNode(fi,
          FlowGraphTraversal.schemaPropagate(p.id, CoreProperties(p.properties)),
          p.id)))
      .flatMap(ptu =>
        Future.sequence(ptu.flatten.map(p =>
          processorApi.update(p, clientId))))
  }

  override def createProcessorConnection(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {
    postAsJson(path = connectionsProcessGroupPath(connectionConfig.flowInstanceId),
      body = FlowConnectionRequest(connectionConfig, clientId))
      .flatMap { response =>
        propagateSchema(connectionConfig.source.id, connectionConfig.flowInstanceId, clientId)
          .map(pis => ConnectionAdapter(response.toObject[ConnectionEntity]))
      }
  }

  def createConnectionToExternalProcessor(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {

    ioPortApi.createOutputPort(connectionConfig.flowInstanceId, clientId)
      .flatMap { oconn =>
        val cc = ConnectionConfig(
          connectionConfig.flowInstanceId,
          connectionConfig.source,
          oconn.config.source,
          connectionConfig.selectedRelationships,
          connectionConfig.availableRelationships
        )
        createStdConnection(cc, clientId)
          .flatMap { c =>
            processorApi.updateProperties(connectionConfig.destination.id,
              Map(ExternalProcessorProperties.ReceiverKey ->
                ExternalProcessorProperties.nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, oconn.config.destination.name),
                ExternalProcessorProperties.RootOutputConnectionIdKey -> oconn.id,
                ExternalProcessorProperties.OutputPortNameKey -> c.config.destination.name),
              clientId)
              .flatMap (_ => propagateSchema(connectionConfig.source.id, c.config.flowInstanceId, clientId)
                .map(pis => c.withConnection(oconn))
              )
          }
      }
  }

  def createConnectionFromExternalProcessor(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {

    ioPortApi.createInputPort(connectionConfig.flowInstanceId, clientId)

      .flatMap { iconn =>
        val cc = ConnectionConfig(
          connectionConfig.flowInstanceId,
          iconn.config.destination,
          connectionConfig.destination
        )
        createStdConnection(cc, clientId)
          .flatMap { c =>
            processorApi.updateProperties(connectionConfig.source.id,
              Map(ExternalProcessorProperties.SenderKey ->
                ExternalProcessorProperties.nifiSenderWithArgs(NifiApiConfig.BaseUiUrl, iconn.config.source.name),
                ExternalProcessorProperties.RootInputConnectionIdKey -> iconn.id,
                ExternalProcessorProperties.InputPortNameKey -> c.config.source.name),
              clientId)
              .flatMap (p => propagateSchema(connectionConfig.source.id, c.config.flowInstanceId, clientId)
                .map(pis => c.withConnection(iconn))
              )
          }
      }
  }

  def createConnectionForInputPortIngestionProcessor(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {

    ioPortApi.createInputPort(connectionConfig.flowInstanceId, clientId)

      .flatMap { iconn =>
        val cc = ConnectionConfig(
          connectionConfig.flowInstanceId,
          iconn.config.destination,
          connectionConfig.destination
        )
        createStdConnection(cc, clientId)
          .flatMap { c =>
            processorApi.updateProperties(connectionConfig.destination.id,
              Map(ExternalProcessorProperties.RootInputConnectionIdKey -> iconn.id,
                ExternalProcessorProperties.InputPortNameKey -> c.config.source.name,
                ExternalProcessorProperties.RootInputPortIdKey -> iconn.config.source.id),
              clientId)
              .flatMap (p => propagateSchema(connectionConfig.destination.id, c.config.flowInstanceId, clientId)
                .map(pis => c.withConnection(iconn))
              )
          }
      }
  }

  override def createStdConnection(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {
    postAsJson(path = connectionsProcessGroupPath(connectionConfig.flowInstanceId),
      body = FlowConnectionRequest(connectionConfig, clientId))
      .map { response =>
        ConnectionAdapter(response.toObject[ConnectionEntity])
      }
  }

  override def update(connection: Connection, clientId: String): Future[Connection] = {
    putAsJson(path = connectionsPath(connection.id),
      body = FlowConnectionRequest(connection, clientId))
      .map { response =>
        ConnectionAdapter(response.toObject[ConnectionEntity])
      }
  }


  override def remove(connection: Connection, clientId: String): Future[Boolean] = {
    ((connection.config.source.componentType, connection.config.destination.componentType) match {
          case (FlowComponent.ProcessorType, FlowComponent.ProcessorType) =>
            emptyQueue(connection.id)
              .flatMap ( _=> removeProcessorConnection(connection, clientId))
          case (FlowComponent.ProcessorType, FlowComponent.ExternalProcessorType) |
               (FlowComponent.ExternalProcessorType, FlowComponent.ProcessorType) =>
            removeExternalProcessorConnection(connection, clientId)
          case ( _ , FlowComponent.InputPortIngestionType) =>
            removeInputPortIngestionProcessorConnection(connection, clientId)
          case (FlowComponent.InputPortType, FlowComponent.InputPortType) =>
            emptyQueue(connection.id)
              .flatMap ( _=> removeRootInputPortConnection(connection, clientId))
          case (FlowComponent.InputPortType, FlowComponent.ProcessorType) =>
            emptyQueue(connection.id)
              .flatMap ( _=> removeProcessorInputPortConnection(connection, clientId))
          case (FlowComponent.OutputPortType, FlowComponent.OutputPortType) =>
            emptyQueue(connection.id)
              .flatMap ( _=> removeRootOutputPortConnection(connection, clientId))
          case (FlowComponent.ProcessorType, FlowComponent.OutputPortType) =>
            emptyQueue(connection.id)
              .flatMap ( _=> removeProcessorOutputPortConnection(connection, clientId))
          case _ => Future(false)
        })
      .flatMap { deleteOk =>
        if (deleteOk)
          Future.sequence(connection.relatedConnections.map(relc => remove(relc, clientId)))
            .map(_.forall(identity))
        else
          Future(false)
      }
      .flatMap(deleteOk =>
        if (deleteOk)
          postRemove(connection, clientId)
        else
          Future(false))
  }

  def postRemove(connection: Connection, clientId: String): Future[Boolean] = {
    (connection.config.source.componentType, connection.config.destination.componentType) match {
      case (_, FlowComponent.ExternalProcessorType) =>
        processorApi.updateProperties(connection.config.destination.id,
          Map(ExternalProcessorProperties.ReceiverKey -> "",
            ExternalProcessorProperties.RootOutputConnectionIdKey -> "",
            ExternalProcessorProperties.OutputPortNameKey -> ""),
          clientId)
          .map(_ != null)
      case (FlowComponent.ExternalProcessorType, _) =>
        processorApi.updateProperties(connection.config.source.id,
          Map(ExternalProcessorProperties.SenderKey -> "",
            ExternalProcessorProperties.RootInputConnectionIdKey -> "",
            ExternalProcessorProperties.InputPortNameKey -> ""),
          clientId)
          .map(_ != null)
      case _ => Future(true) // do nothing
    }
  }

  override def remove(connectionId: String, version: Long, clientId: String): Future[Boolean] = {
    find(connectionId, clientId).flatMap(connection => remove(connection, clientId))
  }

  def removeExternalProcessorConnection(connection: Connection, clientId: String): Future[Boolean] = {
    unPropagateSchema(connection.config.source.id, connection.config.flowInstanceId, clientId)
      .map(pis => true)
  }

  def removeInputPortIngestionProcessorConnection(connection: Connection, clientId: String): Future[Boolean] = {
    unPropagateSchema(connection.config.destination.id, connection.config.flowInstanceId, clientId)
      .map(pis => true)
  }

  def removeRootInputPortConnection(connection: Connection, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, clientId)
      .flatMap { deleteOk =>
        if(deleteOk)
          ioPortApi.deleteInputPort(connection.config.source.id, connection.config.destination.id, clientId)
        else
          Future(false)
      }
  }


  def removeProcessorInputPortConnection(connection: Connection, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, clientId)
  }


  def removeRootOutputPortConnection(connection: Connection, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, clientId)
      .flatMap { deleteOk =>
        if(deleteOk)
          ioPortApi.deleteOutputPort(connection.config.source.id, connection.config.destination.id, clientId)
        else
          Future(false)
      }
  }

  def removeProcessorOutputPortConnection(connection: Connection, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, clientId)
      .flatMap { deleteOk =>
        if(deleteOk)
          processorApi.autoTerminateRelationship(connection).map(_ != null)
        else
          Future(false)
      }
  }

  def unPropagateSchema(startProcessorId: String, flowInstanceId: String, clientId: String): Future[List[ProcessorInstance]] = {
    processorApi.instance(startProcessorId)
      .flatMap(p => FlowApi.instance(flowInstanceId, clientId)
        .map(fi => FlowGraph.executeBreadthFirstFromNode(fi,
          FlowGraphTraversal.schemaUnPropagate(p.id, CoreProperties(p.properties)),
          p.id)))
      .flatMap(ptu =>
        Future.sequence(ptu.flatten.map(p =>
          processorApi.update(p, clientId))))
  }

  def removeProcessorConnection(connection: Connection, clientId: String): Future[Boolean] = {
    unPropagateSchema(connection.config.source.id, connection.config.flowInstanceId, clientId)
      .flatMap(pis =>
        deleteAsJson(path = connectionsPath(connection.id),
          queryParams = Revision.params(connection.version, clientId))
          .flatMap { response =>
            processorApi.autoTerminateRelationship(connection).map(_ != null)
          }
      )
  }

  def removeStdConnection(connection: Connection, clientId: String): Future[Boolean] = {
    deleteAsJson(path = connectionsPath(connection.id),
      queryParams = Revision.params(connection.version, clientId))
      .map { response =>
        ConnectionAdapter(response.toObject[ConnectionEntity]) != null
      }
  }

  def emptyQueue(connectionId: String): Future[Boolean] = {
    postAsJson(path = connectionQueuePath(connectionId))
      .flatMap { response =>
        checkDropRequestUntilFinished(connectionId, DropRequestAdapter(response.toObject[DropRequestEntity]).id)
      }
      .flatMap(dr => deleteDropRequest(connectionId, dr.id))
  }

  def checkDropRequestUntilFinished(connectionId: String, dropRequest: DropRequest): Future[DropRequest] = {
    if(dropRequest.finished)
      Future(dropRequest)
    else
      checkDropRequestUntilFinished(connectionId, dropRequest.id)
  }


  def checkDropRequestUntilFinished(connectionId: String, dropRequestId: String, checkCount: Int = 0): Future[DropRequest] = {
    import scala.concurrent._
    // FIXME: Ugly hack to ensure that the drop request is valid.
    //        Without this the first response of the drop request query
    //        will be that the queue is empty which is incorrect
    blocking { Thread.sleep(1000) }
    getAsJson(path = connectionQueueDropRequestPath(connectionId, dropRequestId))
      .flatMap { response =>
        val dr = DropRequestAdapter(response.toObject[DropRequestEntity])
        if(dr.finished)
          Future(dr)
        else {
          if(checkCount > MaxDropRequestCheckCount)
            throw new IllegalStateException("Could not empty connection queue with id " + connectionId +
              " even after " + MaxDropRequestCheckCount + " tries")
          else
            checkDropRequestUntilFinished(connectionId, dropRequestId, checkCount + 1)
        }
      }
  }

  def deleteDropRequest(connectionId: String, dropRequestId: String): Future[Boolean] = {
    deleteAsJson(path = connectionQueueDropRequestPath(connectionId, dropRequestId))
      .map { response =>
        DropRequestAdapter(response.toObject[DropRequestEntity]).finished
      }
  }
}
