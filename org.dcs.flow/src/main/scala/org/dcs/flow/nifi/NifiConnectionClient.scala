package org.dcs.flow.nifi

import org.apache.nifi.web.api.entity.ConnectionEntity
import org.dcs.api.service.{Connection, ConnectionApiService, ConnectionConfig}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient

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
    deleteAsJson(path = connectionsPath(connectionId),
      queryParams = Revision.params(version, clientId))
      .map { response =>
        response.toObject[Connection] != null
      }
  }
}
