package org.dcs.flow

/**
  * Created by cmathew on 31.10.16.
  */

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.dcs.api.error.RESTException
import org.dcs.commons.JsonSerializerImplicits._
import play.api.http.MimeTypes
import play.api.libs.json.Json
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ahc.AhcWSClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait JsonPlayWSClient extends ApiConfig {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val wsClient = AhcWSClient()

  def defaultHeaders(): List[(String, String)] = {
    ("Content-Type", MimeTypes.JSON) :: Nil
  }

  def get(path: String,
          queryParams: List[(String, String)] = List(),
          headers: List[(String, String)] = List()): Future[WSResponse] = {
    wsClient.url(endpoint(path))
      .withQueryString(queryParams:_*)
      .withHeaders(headers:_*)
      .get
      .map { response =>
        if(response.status >= 400 && response.status < 600)
          throw new RESTException(error(response.status, response.body))
        else
          response
      }
  }


  def getAsJson(path: String,
                queryParams: List[(String, String)] = List(),
                headers: List[(String, String)] = List()): Future[String] = {
    get(path, queryParams, headers)
      .map { response =>
        response.json.toString
      }
  }


  def put[B](path: String,
             body: B = AnyRef,
             queryParams: List[(String, String)] = List(),
             headers: List[(String, String)] = List()): Future[WSResponse] = {
    wsClient.url(endpoint(path))
      .withQueryString(queryParams:_*)
      .withHeaders(defaultHeaders() ++ headers:_*)
      .put(body.toJson)
      .map { response =>
        if(response.status >= 400 && response.status < 600)
          throw new RESTException(error(response.status, response.body))
        else
          response
      }
  }


  def putAsJson[B](path: String,
                   body: B = AnyRef,
                   queryParams: List[(String, String)] = List(),
                   headers: List[(String, String)] = List()): Future[String] = {
    put(path, body, queryParams, headers)
      .map { response =>
        response.json.toString
      }
  }

  def post[B](path: String,
              body: B = AnyRef,
              queryParams: List[(String, String)] = List(),
              headers: List[(String, String)] = List()): Future[WSResponse] = {
    wsClient.url(endpoint(path))
      .withQueryString(queryParams:_*)
      .withHeaders(defaultHeaders() ++ headers:_*)
      .post(body.toJson)
      .map { response =>
        if(response.status >= 400 && response.status < 600)
          throw new RESTException(error(response.status, response.body))
        else
          response
      }
  }


  def postAsJson[B](path: String,
                    body: B = AnyRef,
                    queryParams: List[(String, String)] = List(),
                    headers: List[(String, String)] = List()): Future[String] = {
    post(path, body, queryParams, headers)
      .map { response =>
        response.json.toString
      }
  }

  def delete(path: String,
             queryParams: List[(String, String)] = List(),
             headers: List[(String, String)] = List()): Future[WSResponse] = {
    wsClient.url(endpoint(path))
      .withQueryString(queryParams:_*)
      .withHeaders(headers:_*)
      .delete
      .map { response =>
        if(response.status >= 400 && response.status < 600)
          throw new RESTException(error(response.status, response.body))
        else
          response
      }
  }


  def deleteAsJson(path: String,
                   queryParams: List[(String, String)] = List(),
                   headers: List[(String, String)] = List()): Future[String] = {
    delete(path, queryParams, headers)
      .map { response =>
        response.json.toString
      }
  }

}
