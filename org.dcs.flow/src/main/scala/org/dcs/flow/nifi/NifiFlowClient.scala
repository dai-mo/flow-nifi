package org.dcs.flow.nifi

import javax.ws.rs.core.MediaType

import org.apache.nifi.web.api.entity.{FlowSnippetEntity, ProcessGroupEntity, SnippetEntity, TemplatesEntity}
import org.dcs.api.service.{FlowApiService, FlowInstance, FlowTemplate}
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.flow.nifi.NifiBaseRestClient._

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/05/16.
  */
object NifiProcessorApi extends NifiProcessorClient
  with NifiApiConfig

object NifiFlowClient {
  val TemplateInstancePath = "/controller/process-groups/root/template-instance"
  val TemplatesPath = "/controller/templates"
  val ProcessGroupsPath = "/controller/process-groups"
  val SnippetsPath = "/controller/snippets"

}

trait NifiFlowClient extends FlowApiService with NifiBaseRestClient {
  import NifiFlowClient._

  override def templates(clientId: String):List[FlowTemplate] = {
    val templates = getAsJson(path = TemplatesPath,
      queryParams = (ClientIdKey -> clientId) :: Nil).toObject[TemplatesEntity]
    templates.getTemplates.asScala.map(t => FlowTemplate(t)).toList
  }

  override def instantiate(flowTemplateId:String, clientId: String):FlowInstance = {
    val qp = List(
      ("templateId", flowTemplateId),
      ("originX", "17"),
      ("originY" -> "100")
    )

    val flowSnippet = postAsJson(path = TemplateInstancePath,
      queryParams = (ClientIdKey -> clientId) :: qp,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[FlowSnippetEntity]

    FlowInstance(flowSnippet)

  }

  override def instance(flowInstanceId: String, clientId: String): FlowInstance = {
    val processGroupEntity = getAsJson(path = ProcessGroupsPath + "/" + flowInstanceId,
      queryParams = ("verbose" -> "true") :: (ClientIdKey -> clientId) :: Nil
    ).toObject[ProcessGroupEntity]

    FlowInstance(processGroupEntity)
  }

  def register(flowInstance: FlowInstance, groupId:String, clientId: String): FlowInstance = {
    val qp = List(("linked","true"),
      ("parentGroupId" -> groupId))

    val cMap = flowInstance.connections.map(c => ("connectionIds[]" -> c.id))
    val pMap = flowInstance.processors.map(p => ("processorIds[]" -> p.id))

    val flowSnippet = postAsJson(path = SnippetsPath,
      queryParams = (ClientIdKey -> clientId) :: qp  ++ cMap ++ pMap,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[SnippetEntity]

    FlowInstance(flowSnippet)
  }

  override def remove(flowInstanceId: String, clientId:String): Boolean = {
    val flowInstance = instance(flowInstanceId, clientId)

    val registeredFlowInstance = register(flowInstance, flowInstanceId, clientId)

    val response = deleteAsJson(path = SnippetsPath + "/" + registeredFlowInstance.id,
      queryParams = (ClientIdKey -> clientId) :: Nil)

    response != null
  }
}
