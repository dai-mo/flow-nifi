package org.dcs.flow.nifi

import java.util.UUID
import javax.ws.rs.client.Invocation.Builder

import org.apache.nifi.web.api.entity.Entity
import org.dcs.flow.BaseRestApi

import org.dcs.commons.JsonSerializerImplicits._

/**
  * Created by cmathew on 30/05/16.
  */
object NifiBaseRestApi {
  val RevisionPath = "/controller/revision"
  val clientId = UUID.randomUUID().toString
}

trait NifiBaseRestApi extends BaseRestApi {
  import NifiBaseRestApi._

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
