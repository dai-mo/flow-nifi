package org.dcs.flow.nifi

import javax.ws.rs.client.Invocation.Builder

import org.dcs.flow.BaseRestClient

/**
  * Created by cmathew on 30/05/16.
  */
object NifiBaseRestClient {
  val RevisionPath = "/controller/revision"
  val ClientIdKey = "clientId"
}

trait NifiBaseRestClient extends BaseRestClient {

  private var version = 0

//  def currentVersion(): Long = super.response(RevisionPath).
//    get.
//    readEntity(classOf[String]).
//    toObject[Entity].getRevision.getVersion

  override def response(nifiPath: String,
                        nifiQueryParams: Map[String, String] = Map(),
                        nifiHeaders: List[(String, String)] = List()): Builder = {
    super.response(nifiPath,
      nifiQueryParams,
      nifiHeaders)
  }



}
