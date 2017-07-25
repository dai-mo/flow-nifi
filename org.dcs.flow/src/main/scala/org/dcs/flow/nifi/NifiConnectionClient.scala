package org.dcs.flow.nifi

import org.apache.nifi.web.api.entity.ConnectionEntity
import org.dcs.api.service.{Connection, ConnectionApiService, ConnectionConfig}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.ProcessorApi

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

/**
  * Created by cmathew on 18.04.17.
  */

class NifiConnectionApi extends NifiConnectionClient with NifiApiConfig

object NifiConnectionClient {

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
        deleteAsJson(path = connectionsPath(connectionId),
          queryParams = Revision.params(version, clientId))
          .flatMap { response =>
            ProcessorApi.autoTerminateRelationship(connection).map(_ != null)
          }
      )
  }
}
