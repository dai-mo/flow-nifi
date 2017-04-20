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

  override def templates():Future[List[FlowTemplate]] =
    getAsJson(path = TemplatesPath)
      .map { response =>
        val te = response.toObject[TemplatesEntity]
        te.getTemplates.asScala.map(t => FlowTemplate(t.getTemplate)).toList
      }

  def template(flowTemplateId: String): Future[Option[FlowTemplate]] =
    templates().map { templates =>
      templates.find(ft => ft.id == flowTemplateId)
    }

  override def create(flowName: String, clientId: String): Future[FlowInstance] = {
    createProcessGroup(flowName, ProcessGroupHelper.RootProcessGroup, clientId)
      .map(pg =>  FlowInstance(pg.id, pg.getName, pg.version))
  }

  override def instantiate(flowTemplateId: String, clientId: String): Future[FlowInstance] = {
    val qp = Map(
      "templateId" -> flowTemplateId
    )

    // FIXME: Persist flow instances
    // The following code is a workaround for the problem with nifi not able
    // to persist individual flow instances. The workaround creates a process group
    // under the user process group which isolates the flow instance
    def templateOrError() =
    template(flowTemplateId)
      .map { template =>
        if(template.isDefined)
          template
        else
          throw new RESTException(ErrorConstants.DCS301)
      }

    for {
      t <- templateOrError()
      pg <- createProcessGroup(t.get.name, ProcessGroupHelper.RootProcessGroup, clientId)
      i <- instance(flowTemplateId, pg)
    } yield i
  }

  def instance(flowTemplateId: String, processGroup: ProcessGroup): Future[FlowInstance] =
    postAsJson[InstantiateTemplateRequestEntity](path = templateInstancePath(processGroup.id), body = FlowInstanceRequest(flowTemplateId))
      .map { response =>
        FlowInstance(response.toObject[FlowEntity], processGroup.id, processGroup.getName, processGroup.version)
      }

  override def instance(flowInstanceId: String): Future[FlowInstance] = {
    processGroupVersion(flowInstanceId)
      .flatMap { version =>
        getAsJson(path = flowProcessGroupsPath(flowInstanceId))
          .map { response =>
            FlowInstance(response.toObject[ProcessGroupFlowEntity], version.toLong)
          }
      }
  }

  override def instances(): Future[List[FlowInstance]] = {

    def rootProcessGroup(): Future[ProcessGroupFlowEntity] = {
      getAsJson(path = flowProcessGroupsPath(ProcessGroupHelper.RootProcessGroup))
        .map { response =>
          response.toObject[ProcessGroupFlowEntity]
        }
    }

    def flowInstances(root: ProcessGroupFlowEntity): Future[List[FlowInstance]] = {
      Future.sequence(root.getProcessGroupFlow.getFlow.getProcessGroups.asScala.map(
        pge => instance(pge.getComponent.getId)
      ).toList)
    }


    for {
      root <- rootProcessGroup
      instanceList <- flowInstances(root)
    } yield instanceList
  }

  override def start(flowInstanceId: String): Future[FlowInstance] = {
    putAsJson[FlowInstanceStartRequest](path = flowProcessGroupsPath(flowInstanceId),
      body = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateRunning))
      .map { response =>
        response.toObject[FlowInstance]
      }
  }

  override def stop(flowInstanceId: String): Future[FlowInstance] = {
    putAsJson[FlowInstanceStartRequest](path = flowProcessGroupsPath(flowInstanceId),
      body = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateStopped))
      .map { response =>
        response.toObject[FlowInstance]
      }
  }

  override def remove(flowInstanceId: String, version: Long, clientId: String): Future[Boolean] = {
    deleteAsJson(path = processGroupsPath(flowInstanceId),
    queryParams = Revision.params(version, clientId))
      .map { response =>
        response != null
      }
  }

  def createProcessGroup(name: String, processGroupId: String, clientId: String): Future[ProcessGroup] = {
    postAsJson[ProcessGroupEntity](path = processGroupsPath(processGroupId) + "/process-groups",
      body = FlowInstanceContainerRequest(name  + ProcessGroupHelper.NameIdDelimiter + UUID.randomUUID().toString, clientId))
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
