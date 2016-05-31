package org.dcs.flow.nifi

import java.util.UUID
import javax.ws.rs.client.Invocation.Builder

import org.apache.nifi.web.api.entity.Entity
import org.dcs.flow.BaseRestClient

import org.dcs.commons.JsonSerializerImplicits._

/**
  * Created by cmathew on 30/05/16.
  */
object NifiBaseRestClient {
  val RevisionPath = "/controller/revision"
  val clientId = UUID.randomUUID().toString
}

trait NifiBaseRestClient extends BaseRestClient {
  import NifiBaseRestClient._

  private var version = 0

  def currentVersion(): Long = super.response(RevisionPath).
    get.
    readEntity(classOf[String]).
    toObject[Entity].getRevision.getVersion

  override def response(path: String,
                        queryParams: Map[String, String],
                        headers: Map[String, String]): Builder = {
    super.response(path,
      queryParams + ("clientId" -> clientId) + ("version" -> currentVersion.toString),
      headers)
  }
}
