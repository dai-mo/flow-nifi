package org.dcs.flow.nifi

import org.apache.nifi.web.api.entity.ConnectionEntity
import org.dcs.api.processor.{CoreProperties, ExternalProcessorProperties}
import org.dcs.api.service.{Connectable, Connection, ConnectionApiService, ConnectionConfig, FlowComponent}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.{FlowApi, FlowGraph, FlowGraphTraversal}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

/**
  * Created by cmathew on 18.04.17.
  */

class NifiConnectionApi extends NifiConnectionClient with NifiApiConfig

object NifiConnectionClient {

  val ioPortApi = new NifiIOPortApi
  val processorApi = new NifiProcessorApi

  def connectionsProcessGroupPath(processGroupId: String): String =
    "/process-groups/" + processGroupId + "/connections"

  def connectionsPath(connectionId: String): String =
    "/connections/" + connectionId
}

trait NifiConnectionClient extends ConnectionApiService with JerseyRestClient {

  import NifiConnectionClient._


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
      case (FlowComponent.InputPortType, FlowComponent.InputPortType) |
           (FlowComponent.OutputPortType, FlowComponent.OutputPortType) |
           (FlowComponent.InputPortType, FlowComponent.ProcessorType) |
           (FlowComponent.ProcessorType, FlowComponent.OutputPortType) =>
        createStdConnection(connectionConfig, clientId)
      case _ => throw new IllegalArgumentException("Cannot connect source of type " + connectionConfig.source.componentType +
        " to destination of type " + connectionConfig.destination.componentType)
    }
  }

  override def createProcessorConnection(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {
    postAsJson(path = connectionsProcessGroupPath(connectionConfig.flowInstanceId),
      body = FlowConnectionRequest(connectionConfig, clientId))
      .flatMap { response =>
        processorApi.instance(connectionConfig.source.id)
          .flatMap(p => FlowApi.instance(connectionConfig.flowInstanceId)
            .map(fi => FlowGraph.executeBreadthFirstFromNode(fi,
              FlowGraphTraversal.schemaPropagate(p.id, CoreProperties(p.properties)),
              p.id)))
          .map(ptu =>
            ptu.map(p =>
              p.map(p =>
                processorApi.update(p, clientId))))
          .map(pis => ConnectionAdapter(response.toObject[ConnectionEntity]))
      }
  }

  def createConnectionToExternalProcessor(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {

    ioPortApi.createOutputPort(connectionConfig.flowInstanceId, clientId)
      .flatMap { oconn =>
        processorApi.updateProperties(connectionConfig.destination.id,
          Map(ExternalProcessorProperties.ReceiverKey ->
            ExternalProcessorProperties.nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, oconn.config.destination.name),
            ExternalProcessorProperties.RootOutputConnectionKey -> oconn.toJson),
          clientId)
          .flatMap { _ =>
            val cc = ConnectionConfig(
              connectionConfig.flowInstanceId,
              connectionConfig.source,
              oconn.config.source,
              connectionConfig.selectedRelationships,
              connectionConfig.availableRelationships
            )
            createStdConnection(cc, clientId)
          }
          .map(_.withConnection(oconn))

      }
  }

  def createConnectionFromExternalProcessor(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {

    ioPortApi.createInputPort(connectionConfig.flowInstanceId, clientId)
      .flatMap { iconn =>
        processorApi.updateProperties(connectionConfig.source.id,
          Map(ExternalProcessorProperties.SenderKey ->
            ExternalProcessorProperties.nifiSenderWithArgs(NifiApiConfig.BaseUiUrl, iconn.config.source.name),
            ExternalProcessorProperties.RootInputConnectionKey -> iconn.toJson),
          clientId)
          .flatMap { processor =>
            val cc = ConnectionConfig(
              connectionConfig.flowInstanceId,
              iconn.config.destination,
              connectionConfig.destination,
              connectionConfig.selectedRelationships,
              connectionConfig.availableRelationships
            )
            createStdConnection(cc, clientId)
          }
          .map(_.withConnection(iconn))
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


  override def remove(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    ((connection.config.source.componentType, connection.config.destination.componentType) match {
      case (FlowComponent.ProcessorType, FlowComponent.ProcessorType) =>
        removeProcessorConnection(connection, version, clientId)
      case (FlowComponent.ProcessorType, FlowComponent.ExternalProcessorType) |
           (FlowComponent.ExternalProcessorType, FlowComponent.ProcessorType) => Future(true)
      case (FlowComponent.InputPortType, FlowComponent.InputPortType) =>
        removeRootInputPortConnection(connection, version, clientId)
      case (FlowComponent.InputPortType, FlowComponent.ProcessorType) =>
        removeProcessorInputPortConnection(connection, version, clientId)
      case (FlowComponent.OutputPortType, FlowComponent.OutputPortType) =>
        removeRootOutputPortConnection(connection, version, clientId)
      case (FlowComponent.ProcessorType, FlowComponent.OutputPortType) =>
        removeProcessorOutputPortConnection(connection, version, clientId)
      case _ => Future(false)
    })
      .flatMap { deleteOk =>
        if (deleteOk)
          Future.sequence(connection.relatedConnections.map(relc => remove(relc, version, clientId)))
            .map(_.forall(identity))
        else
          Future(false)
      }
      .flatMap(deleteOk =>
        if (deleteOk)
          postRemove(connection, version, clientId)
        else
          Future(false))
  }

  def postRemove(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    (connection.config.source.componentType, connection.config.destination.componentType) match {
      case (_, FlowComponent.ExternalProcessorType) =>
        processorApi.updateProperties(connection.config.destination.id,
          Map(ExternalProcessorProperties.ReceiverKey -> "",
            ExternalProcessorProperties.RootOutputConnectionKey -> ""),
          clientId)
          .map(_ != null)
      case (FlowComponent.ExternalProcessorType, _) =>
        processorApi.updateProperties(connection.config.source.id,
          Map(ExternalProcessorProperties.SenderKey -> "",
            ExternalProcessorProperties.RootInputConnectionKey -> ""),
          clientId)
          .map(_ != null)
      case _ => Future(true) // do nothing
    }
  }

  override def remove(connectionId: String, version: Long, clientId: String): Future[Boolean] = {
    find(connectionId, clientId).flatMap(connection => remove(connection, version, clientId))
  }

  def removeExternalProcessorConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    Future.sequence(connection.relatedConnections.map(connection => remove(connection, version, clientId)))
      .map(_.forall(identity))
  }

  def removeRootInputPortConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, version, clientId)
      .flatMap { deleteOk =>
        if(deleteOk)
          ioPortApi.deleteInputPort(connection.config.source.id, connection.config.destination.id, connection.version, clientId)
        else
          Future(false)
      }
  }


  def removeProcessorInputPortConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, version, clientId)
  }


  def removeRootOutputPortConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, version, clientId)
      .flatMap { deleteOk =>
        if(deleteOk)
          ioPortApi.deleteOutputPort(connection.config.destination.id, connection.config.source.id, version, clientId)
        else
          Future(false)
      }
  }

  def removeProcessorOutputPortConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    removeStdConnection(connection, version, clientId)
      .flatMap { deleteOk =>
        if(deleteOk)
          processorApi.autoTerminateRelationship(connection).map(_ != null)
        else
          Future(false)
      }
  }

  def removeProcessorConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    processorApi.instance(connection.config.source.id)
      .flatMap(p => FlowApi.instance(connection.config.flowInstanceId)
        .map(fi => FlowGraph.executeBreadthFirstFromNode(fi,
          FlowGraphTraversal.schemaUnPropagate(p.id, CoreProperties(p.properties)),
          p.id)))
      .map(ptu =>
        ptu.map(p =>
          p.map(p =>
            processorApi.update(p, clientId))))
      .flatMap(pis =>
        deleteAsJson(path = connectionsPath(connection.id),
          queryParams = Revision.params(version, clientId))
          .flatMap { response =>
            processorApi.autoTerminateRelationship(connection).map(_ != null)
          }
      )
  }

  def removeStdConnection(connection: Connection, version: Long, clientId: String): Future[Boolean] = {
    deleteAsJson(path = connectionsPath(connection.id),
      queryParams = Revision.params(version, clientId))
      .map { response =>
        ConnectionAdapter(response.toObject[ConnectionEntity]) != null
      }
  }
}
