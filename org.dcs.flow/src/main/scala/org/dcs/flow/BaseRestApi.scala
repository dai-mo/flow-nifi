package org.dcs.flow

import javax.ws.rs.core.Response
import javax.ws.rs.core.MediaType
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.client.Invocation.Builder
import javax.ws.rs.client.Entity

trait BaseRestApi extends ApiConfig {  
  
  def response(path: String, queryParams: Option[Map[String, String]] = None): Builder = {
    val target = ClientBuilder.newClient.target(baseUrl)
    if(queryParams != None)  queryParams.get.foreach(x => target.queryParam(x._1, x._2))    
    target.path(path).request(MediaType.APPLICATION_JSON)      
  }
  
  
  def get(path: String, queryParams: Option[Map[String, String]] = None): Response = {
    response(path, queryParams).get
  }
  
  
  def getAsJson(path: String, queryParams: Option[Map[String, String]] = None): String = {
    get(path, queryParams).readEntity(classOf[String])
  }

  def put[T](path: String, obj: T = Entity.json(null), queryParams: Option[Map[String, String]] = None): Response = {
    response(path, queryParams).put(Entity.entity(obj, MediaType.APPLICATION_JSON))
  }
  
  def putAsJson[T](path: String, obj: T = Entity.json(null), queryParams: Option[Map[String, String]] = None): String = {
    put(path, obj, queryParams).readEntity(classOf[String])
  }
  
   def post[T](path: String, obj: T = Entity.json(null), queryParams: Option[Map[String, String]] = None): Response = {
    response(path, queryParams).post(Entity.entity(obj, MediaType.APPLICATION_JSON))
  }
  
  def postAsJson[T](path: String, obj: T = Entity.json(null), queryParams: Option[Map[String, String]] = None): String = {
    post(path, obj, queryParams).readEntity(classOf[String])
  }
  
  def delete(path: String, queryParams: Option[Map[String, String]] = None): Response = {
    response(path, queryParams).delete
  }
  
  def deleteAsJson(path: String, queryParams: Option[Map[String, String]] = None): String = {
    delete(path).readEntity(classOf[String])
  }
}