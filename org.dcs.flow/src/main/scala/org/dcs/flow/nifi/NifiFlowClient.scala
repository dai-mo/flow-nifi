package org.dcs.flow.nifi

import java.util.UUID
import javax.ws.rs.core.MediaType

import org.apache.nifi.web.api.entity._
import org.dcs.api.error.{ErrorConstants, RESTException}
import org.dcs.api.service.{Connection, FlowApiService, FlowInstance, FlowTemplate, ProcessorInstance}
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.flow.ProcessorApi
import org.dcs.flow.nifi.NifiBaseRestClient._
import org.dcs.flow.nifi.internal.ProcessGroup

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/05/16.
  */

class NifiFlowApi extends NifiFlowClient with NifiApiConfig



object NifiFlowClient {

  val TemplatesPath = "/controller/templates"

  val SnippetsPath = "/controller/snippets"

  def templateInstancePath(processGroupId: String) =
    "/controller/process-groups/" + processGroupId + "/template-instance"

  def processGroupsPath(processGroupId: String) =
    "/controller/process-groups/" + processGroupId + "/process-group-references"

}

trait NifiFlowClient extends FlowApiService with NifiBaseRestClient {
  import NifiFlowClient._

  override def templates(userId: String):List[FlowTemplate] = {
    val templates = getAsJson(path = TemplatesPath,
      queryParams = (ClientIdKey -> userId) :: Nil).toObject[TemplatesEntity]
    templates.getTemplates.asScala.map(t => FlowTemplate(t)).toList
  }

  override def instantiate(flowTemplateId: String, userId: String, authToken: String):FlowInstance = {
    val qp = List(
      "templateId" -> flowTemplateId,
      "originX" -> "17",
      "originY" -> "100",
      ClientIdKey -> userId
    )

    // FIXME: Persist flow instances
    // The following code is a workaround for the problem with nifi not able
    // to persist individual flow instances. The workaround creates a process group
    // under the user process group which isolates the flow instance

    if (templates(userId).exists(ft => ft.id == flowTemplateId)) {
      val processGroupId = UUID.randomUUID().toString
      val processGroup: ProcessGroup = createProcessGroup(processGroupId, userId, authToken)
      val flowSnippetEntity = postAsJson(path = templateInstancePath(processGroup.id),
        queryParams = qp,
        contentType = MediaType.APPLICATION_FORM_URLENCODED
      ).toObject[FlowSnippetEntity]

      FlowInstance(flowSnippetEntity, processGroup.id)
    } else {
      throw new RESTException(ErrorConstants.DCS301)
    }
  }

  override def instance(flowInstanceId: String, userId: String, authToken: String): FlowInstance = {
    val qp = List(
      "verbose" -> "true",
      ClientIdKey -> userId
    )
    val processGroupEntity = getAsJson(path = processGroupsPath(userId) + "/" + flowInstanceId,
      queryParams = qp
    ).toObject[ProcessGroupEntity]

    FlowInstance(processGroupEntity)
  }

  override def instances(userId: String, authToken: String): List[FlowInstance] = {
    val qp = List(
      "verbose" -> "true",
      "process-group-id" -> userId,
      ClientIdKey -> userId
    )
    val processGroupsEntity = getAsJson(path = processGroupsPath(userId),
      queryParams = qp
    ).toObject[ProcessGroupsEntity]

    processGroupsEntity.getProcessGroups.asScala.map(pge => FlowInstance(pge)).toList
  }

  def start(flowInstanceId: String, userId: String, authToken: String): List[ProcessorInstance] = {
    val flowInstance = instance(flowInstanceId, userId, authToken)
    flowInstance.processors.map(p => ProcessorApi.start(p.id, flowInstanceId))
  }

  def stop(flowInstanceId: String, userId: String, authToken: String): List[ProcessorInstance] = {
    val flowInstance = instance(flowInstanceId, userId, authToken)
    flowInstance.processors.map(p => ProcessorApi.stop(p.id, flowInstanceId))
  }

  override def remove(flowInstanceId: String, userId: String, authToken: String): Boolean = {
    val response = deleteAsJson(path = processGroupsPath(userId) + "/" + flowInstanceId,
      queryParams = (ClientIdKey -> userId) :: Nil)

    response != null
  }


  def createProcessGroup(name: String, userId: String, authToken: String): ProcessGroup = {
    val qp = List(
      "name" -> name,
      "x" -> "17",
      "y" -> "100",
      "process-group-id" -> userId,
      ClientIdKey -> userId
    )

    val processGroupEntity = postAsJson(path = processGroupsPath(userId),
      queryParams = qp,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[ProcessGroupEntity]

    ProcessGroup(processGroupEntity)
  }

  def register(flowSnippetEntity: FlowSnippetEntity, userId: String, authToken: String): SnippetEntity = {
    val qp = List(
      "linked" -> "true",
      "parentGroupId" -> userId,
      ClientIdKey -> userId
    )

    val cMap = flowSnippetEntity.getContents.getConnections.asScala.map(c => "connectionIds[]" -> c.getId)
    val pMap = flowSnippetEntity.getContents.getProcessors.asScala.map(p => "processorIds[]" -> p.getId)

    val snippetEntity = postAsJson(path = SnippetsPath,
      queryParams = qp  ++ cMap ++ pMap,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[SnippetEntity]

    snippetEntity
  }

  def register(flowInstance: FlowInstance, userId: String, authToken: String):FlowInstance = {
    val qp = List(
      "linked" -> "true",
      "parentGroupId" -> userId,
      ClientIdKey -> userId
    )

    val cMap = flowInstance.connections.map(c => "connectionIds[]" -> c.id)
    val pMap = flowInstance.processors.map(p => "processorIds[]" -> p.id)

    val flowSnippet = postAsJson(path = SnippetsPath,
      queryParams = qp  ++ cMap ++ pMap,
      contentType = MediaType.APPLICATION_FORM_URLENCODED
    ).toObject[SnippetEntity]

    FlowInstance(flowSnippet)
  }

  def removeSnippet(flowInstanceId: String, userId: String, authToken: String): Boolean = {
    val flowInstance = instance(flowInstanceId, userId, authToken)

    val registeredFlowInstance = register(flowInstance, userId, authToken)

    val response = deleteAsJson(path = SnippetsPath + "/" + registeredFlowInstance.id,
      queryParams = (ClientIdKey -> userId) :: Nil)

    response != null
  }



}
