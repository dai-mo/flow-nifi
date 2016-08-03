package org.dcs.flow

import javax.ws.rs.client.Invocation.Builder
import javax.ws.rs.client.{ClientBuilder, ClientRequestFilter, Entity}
import javax.ws.rs.core.{Form, MediaType, Response}

import org.dcs.api.error.{ErrorConstants, RESTException}

import scala.util.control.NonFatal

trait BaseRestClient extends ApiConfig {

  val client = ClientBuilder.newClient()

  def response(path: String,
               queryParams: List[(String, String)] = List(),
               headers: List[(String, String)]= List()
              ): Builder = {
    var target = client.target(baseUrl)
    if(!queryParams.isEmpty)  queryParams.foreach(x => target = target.queryParam(x._1, x._2))

    var builder = target.path(path).request
    if(!headers.isEmpty)  headers.foreach(x => builder = builder.header(x._1, x._2))

    builder
  }

  def requestFilter(requestFilter: ClientRequestFilter): Unit = {
    client.register(requestFilter, 100)
  }

  def get(path: String,
          queryParams: List[(String, String)] = List(),
          headers: List[(String, String)] = List()): Response = {
    var res: Response = null
    try {
      res = response(path, queryParams, headers).get
    } catch {
      case NonFatal(t) =>
        throw new RESTException(ErrorConstants.DCS201.withErrorMessage(t.getMessage))
    }
    if (res.getStatus >= 400 && res.getStatus < 600) throw new RESTException(error(res))
    res
  }


  def getAsJson(path: String,
                queryParams: List[(String, String)] = List(),
                headers: List[(String, String)] = List()): String = {
    get(path, queryParams, headers).readEntity(classOf[String])
  }

  def put[T](path: String,
             obj: T = new Form,
             queryParams: List[(String, String)] = List(),
             headers: List[(String, String)] = List(),
             contentType: String = MediaType.APPLICATION_JSON): Response = {
    var res: Response = null
    try {
      res = response(path, queryParams, headers).put(Entity.entity(obj, contentType))
    } catch {
      case NonFatal(t) =>
        throw new RESTException(ErrorConstants.DCS201.withErrorMessage(t.getMessage))
    }
    if(res.getStatus >= 400 && res.getStatus < 600) throw new RESTException(error(res))
    res
  }

  def putAsJson[T](path: String,
                   obj: T = new Form,
                   queryParams: List[(String, String)] = List(),
                   headers: List[(String, String)] = List(),
                   contentType: String = MediaType.APPLICATION_JSON): String = {
    put(path, obj, queryParams, headers, contentType).readEntity(classOf[String])
  }

  def post[T](path: String,
              obj: T = new Form,
              queryParams: List[(String, String)] = List(),
              headers: List[(String, String)] = List(),
              contentType: String = MediaType.APPLICATION_JSON): Response = {
    var res: Response = null
    try {
      res = response(path, queryParams, headers).post(Entity.entity(obj, contentType))
    } catch {
      case NonFatal(t) =>
        throw new RESTException(ErrorConstants.DCS201.withErrorMessage(t.getMessage))
    }
    if(res.getStatus >= 400 && res.getStatus < 600) throw new RESTException(error(res))
    res
  }

  def postAsJson[T](path: String,
                    obj: T = new Form,
                    queryParams: List[(String, String)] = List(),
                    headers: List[(String, String)] = List(),
                    contentType: String = MediaType.APPLICATION_JSON): String = {
    post(path, obj, queryParams, headers, contentType).readEntity(classOf[String])
  }

  def delete(path: String,
             queryParams: List[(String, String)] = List(),
             headers: List[(String, String)] = List()): Response = {
    var res: Response = null
    try {
      res  = response(path, queryParams, headers).delete
    } catch {
      case NonFatal(t) =>
        throw new RESTException(ErrorConstants.DCS201.withErrorMessage(t.getMessage))
    }
    if(res.getStatus >= 400 && res.getStatus < 600) throw new RESTException(error(res))
    res
  }

  def deleteAsJson(path: String,
                   queryParams: List[(String, String)] = List(),
                   headers: List[(String, String)] = List()): String = {
    delete(path, queryParams, headers).readEntity(classOf[String])
  }


}