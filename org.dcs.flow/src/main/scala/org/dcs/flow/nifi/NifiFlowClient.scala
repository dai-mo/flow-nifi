package org.dcs.flow.nifi

import java.util.UUID

import org.apache.nifi.web.api.entity._
import org.dcs.api.error.{ErrorConstants, RESTException}
import org.dcs.api.service.{FlowApiService, FlowInstance, FlowTemplate, ProcessorInstance}
import org.dcs.commons.JsonSerializerImplicits._
import org.dcs.flow.ProcessorApi
import org.dcs.flow.nifi.NifiBaseRestClient._
import org.dcs.flow.nifi.NifiFlowGraph.FlowGraphNode
import org.dcs.flow.nifi.internal.{ProcessGroup, ProcessGroupHelper}

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/05/16.
  */

class NifiFlowApi extends NifiFlowClient with NifiApiConfig

object NifiFlowClient {

  val TemplatesPath = "/flow/templates"

  val SnippetsPath = "/controller/snippets"

  def templateInstancePath(processGroupId: String) =
    "/process-groups/" + processGroupId + "/template-instance"

  def processGroupsPath(processGroupId: String) =
    "/process-groups/" + processGroupId

  def flowProcessGroupsPath(processGroupId: String) =
    "/flow/process-groups/" + processGroupId

}

trait NifiFlowClient extends FlowApiService with NifiBaseRestClient {
  import NifiFlowClient._

  override def templates(userId: String):List[FlowTemplate] = {
    val templates = getAsJson(path = TemplatesPath).toObject[TemplatesEntity]
    templates.getTemplates.asScala.map(t => FlowTemplate(t.getTemplate)).toList
  }

  override def instantiate(flowTemplateId: String, userId: String, authToken: String):FlowInstance = {
    val qp = Map(
      "templateId" -> flowTemplateId
    )

    // FIXME: Persist flow instances
    // The following code is a workaround for the problem with nifi not able
    // to persist individual flow instances. The workaround creates a process group
    // under the user process group which isolates the flow instance
    val template = templates(userId).find(ft => ft.id == flowTemplateId)
    if (template.isDefined) {
      val processGroupNameId = UUID.randomUUID().toString
      val processGroup: ProcessGroup =
        createProcessGroup(template.get.name + ProcessGroupHelper.NameIdDelimiter + processGroupNameId,
          userId,
          authToken)
      val flowEntity = postAsJson(path = templateInstancePath(processGroup.id),
        obj = FlowInstanceRequest(flowTemplateId).toJson
      ).toObject[FlowEntity]

      FlowInstance(flowEntity, processGroup.id, processGroup.getName)
    } else {
      throw new RESTException(ErrorConstants.DCS301)
    }
  }

  override def instance(flowInstanceId: String, userId: String, authToken: String): FlowInstance = {

    val processGroupFlowEntity = getAsJson(path = flowProcessGroupsPath(flowInstanceId)
    ).toObject[ProcessGroupFlowEntity]

    FlowInstance(processGroupFlowEntity)
  }

  override def instances(userId: String, authToken: String): List[FlowInstance] = {

    val processGroupFlowEntity = getAsJson(path = flowProcessGroupsPath(userId)).
      toObject[ProcessGroupFlowEntity]

    processGroupFlowEntity.getProcessGroupFlow.getFlow.getProcessGroups.asScala.map(pge => FlowInstance(pge)).toList
  }

  override def start(flowInstanceId: String, userId: String, authToken: String): Boolean = {

  val response = putAsJson(path = flowProcessGroupsPath(flowInstanceId),
    obj = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateRunning).toJson).
    toObject[FlowInstanceStartRequest]

   response.state ==  NifiProcessorClient.StateRunning
  }

  override def stop(flowInstanceId: String, userId: String, authToken: String): Boolean = {

    val response = putAsJson(path = flowProcessGroupsPath(flowInstanceId),
      obj = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateStopped).toJson).
      toObject[FlowInstanceStartRequest]

    response.state ==  NifiProcessorClient.StateStopped
  }

  override def remove(flowInstanceId: String, userId: String, authToken: String): Boolean = {

    val qp = Map(
      "version" -> processGroupVersion(flowInstanceId),
      ClientIdKey -> userId
    )
    val response = deleteAsJson(path = processGroupsPath(flowInstanceId),
      queryParams = qp)

    response != null
  }


  // ---- Helper Methods -----
  def createProcessGroup(name: String, userId: String, authToken: String): ProcessGroup = {
    val processGroupEntity = postAsJson(path = processGroupsPath(userId)  + "/process-groups",
      obj = FlowInstanceContainerRequest(name, userId).toJson
    ).toObject[ProcessGroupEntity]

    ProcessGroup(processGroupEntity)
  }

  def processGroupVersion(flowInstanceId: String): String = {

    getAsJson(path = processGroupsPath(flowInstanceId)).
      toObject[ProcessGroupEntity].
      getRevision.
      getVersion.
      toString

  }
}
