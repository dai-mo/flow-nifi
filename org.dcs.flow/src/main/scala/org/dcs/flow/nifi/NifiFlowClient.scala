package org.dcs.flow.nifi

import java.util.UUID

import org.apache.nifi.web.api.entity._
import org.dcs.api.service.{FlowApiService, FlowInstance, FlowTemplate}
import org.dcs.commons.error.{ErrorConstants, RESTException}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.nifi.internal.{ProcessGroup, ProcessGroupHelper}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

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

trait NifiFlowClient extends FlowApiService with JerseyRestClient {
  import NifiFlowClient._

  override def templates(userId: String):Future[List[FlowTemplate]] =
    getAsJson(path = TemplatesPath)
      .map { response =>
        val te = response.toObject[TemplatesEntity]
        te.getTemplates.asScala.map(t => FlowTemplate(t.getTemplate)).toList
      }

  def template(flowTemplateId: String, userId: String): Future[Option[FlowTemplate]] =
    templates(userId).map { templates =>
      templates.find(ft => ft.id == flowTemplateId)
    }

  override def instantiate(flowTemplateId: String, userId: String, authToken: String): Future[FlowInstance] = {
    val qp = Map(
      "templateId" -> flowTemplateId
    )

    // FIXME: Persist flow instances
    // The following code is a workaround for the problem with nifi not able
    // to persist individual flow instances. The workaround creates a process group
    // under the user process group which isolates the flow instance
    def templateOrError() =
    template(flowTemplateId, userId)
      .map { template =>
        if(template.isDefined)
          template
        else
          throw new RESTException(ErrorConstants.DCS301)
      }

    for {
      t <- templateOrError()
      pg <- createProcessGroup(t.get.name + ProcessGroupHelper.NameIdDelimiter + UUID.randomUUID().toString,
        userId,
        authToken)
      i <- instance(flowTemplateId, pg)
    } yield i
  }

  def instance(flowTemplateId: String, processGroup: ProcessGroup) =
    postAsJson[InstantiateTemplateRequestEntity](path = templateInstancePath(processGroup.id), body = FlowInstanceRequest(flowTemplateId))
      .map { response =>
        FlowInstance(response.toObject[FlowEntity], processGroup.id, processGroup.getName)
      }

  override def instance(flowInstanceId: String,
                        userId: String,
                        authToken: String): Future[FlowInstance] = {
    getAsJson(path = flowProcessGroupsPath(flowInstanceId))
      .map { response =>
        FlowInstance(response.toObject[ProcessGroupFlowEntity])
      }
  }



  override def instances(userId: String, authToken: String): Future[List[FlowInstance]] = {

    def rootProcessGroup(): Future[ProcessGroupFlowEntity] = {
      getAsJson(path = flowProcessGroupsPath(userId))
        .map { response =>
          response.toObject[ProcessGroupFlowEntity]
        }
    }

    def flowInstances(root: ProcessGroupFlowEntity): Future[List[FlowInstance]] = {
      Future.sequence(root.getProcessGroupFlow.getFlow.getProcessGroups.asScala.map(
        pge => instance(pge.getComponent.getId, userId: String, authToken: String)
      ).toList)
    }


    for {
      root <- rootProcessGroup
      instanceList <- flowInstances(root)
    } yield instanceList
  }

  override def start(flowInstanceId: String, userId: String, authToken: String): Future[Boolean] = {
    putAsJson[FlowInstanceStartRequest](path = flowProcessGroupsPath(flowInstanceId),
      body = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateRunning))
      .map { response =>
        response.toObject[FlowInstanceStartRequest].state ==  NifiProcessorClient.StateRunning
      }
  }

  override def stop(flowInstanceId: String, userId: String, authToken: String): Future[Boolean] = {
    putAsJson[FlowInstanceStartRequest](path = flowProcessGroupsPath(flowInstanceId),
      body = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateStopped))
      .map { response =>
        response.toObject[FlowInstanceStartRequest].state == NifiProcessorClient.StateStopped
      }
  }

  override def remove(flowInstanceId: String, userId: String, authToken: String): Future[Boolean] = {
    processGroupVersion(flowInstanceId)
      .flatMap { version =>
        val qp = ("version", version) :: (NifiApiConfig.ClientIdKey, userId) :: Nil
        deleteAsJson(path = processGroupsPath(flowInstanceId),
          queryParams = qp)
          .map { response =>
            response != null
          }
      }
  }

  def createProcessGroup(name: String, processGroupId: String, authToken: String): Future[ProcessGroup] = {
    postAsJson[ProcessGroupEntity](path = processGroupsPath(processGroupId) + "/process-groups",
      body = FlowInstanceContainerRequest(name, processGroupId))
      .map { response =>
        ProcessGroup(response.toObject[ProcessGroupEntity])
      }
  }

  def processGroupVersion(flowInstanceId: String): Future[String] = {
    getAsJson(path = processGroupsPath(flowInstanceId))
      .map { response =>
        response.toObject[ProcessGroupEntity].getRevision.getVersion.toString
      }
  }
}
