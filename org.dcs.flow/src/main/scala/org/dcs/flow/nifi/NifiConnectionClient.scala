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
        Connection(response.toObject[ConnectionEntity])
      }
  }

  override def create(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {
    (connectionConfig.source.componentType, connectionConfig.destination.componentType) match {
      case (FlowComponent.ProcessorType, FlowComponent.ProcessorType) =>
        createProcessorConnection(connectionConfig, clientId)
      case (FlowComponent.ProcessorType, FlowComponent.ExternalProcessorType) =>
        createConnectionToExternalProcessor(connectionConfig, clientId)
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
          .map(pis => Connection(response.toObject[ConnectionEntity]))
      }
  }

  def createConnectionToExternalProcessor(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {

    ioPortApi.createOutputPort(connectionConfig.flowInstanceId, clientId)
      .flatMap { ioportconn =>
        processorApi.updateProperties(connectionConfig.destination.id,
          Map(ExternalProcessorProperties.ReceiverKey ->
            ExternalProcessorProperties.nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, ioportconn._1.name)),
          clientId)
          .flatMap { processor =>
            val cc = ConnectionConfig(
              connectionConfig.flowInstanceId,
              connectionConfig.source,
              ioportconn._2.config.source,
              connectionConfig.selectedRelationships,
              connectionConfig.availableRelationships
            )
            createStdConnection(cc, clientId)
          }
      }
  }

  override def createStdConnection(connectionConfig: ConnectionConfig, clientId: String): Future[Connection] = {
    postAsJson(path = connectionsProcessGroupPath(connectionConfig.flowInstanceId),
      body = FlowConnectionRequest(connectionConfig, clientId))
      .map { response =>
        Connection(response.toObject[ConnectionEntity])
      }
  }

  override def update(connection: Connection, clientId: String): Future[Connection] = {
    putAsJson(path = connectionsPath(connection.id),
      body = FlowConnectionRequest(connection, clientId))
      .map { response =>
        Connection(response.toObject[ConnectionEntity])
      }
  }

  override def remove(connectionId: String, version: Long, clientId: String): Future[Boolean] = {
    find(connectionId, clientId)
      .flatMap(connection =>
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
            deleteAsJson(path = connectionsPath(connectionId),
              queryParams = Revision.params(version, clientId))
              .flatMap { response =>
                processorApi.autoTerminateRelationship(connection).map(_ != null)
              }
          )
      )
  }
}
