package org.dcs.flow.nifi

import java.util.UUID

import org.apache.nifi.web.api.entity._
import org.dcs.api.processor.{ExternalProcessorProperties, RemoteProcessor}
import org.dcs.api.service.{Connection, FlowApiService, FlowInstance, FlowTemplate, ProcessorInstance}
import org.dcs.api.util.NameId
import org.dcs.commons.Control
import org.dcs.commons.error.{ErrorConstants, HttpException}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.nifi.internal.ProcessGroup
import org.glassfish.jersey.filter.LoggingFilter

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

/**
  * Created by cmathew on 30/05/16.
  */

class NifiFlowApi extends NifiFlowClient with NifiApiConfig

object NifiFlowClient {

  val connectionApi = new NifiConnectionApi

  val processorApi = new NifiProcessorApi

  val ioPortApi = new NifiIOPortApi

  val TemplatesPath = "/flow/templates"

  val SnippetsPath = "/controller/snippets"

  def templateInstancePath(processGroupId: String) =
    "/process-groups/" + processGroupId + "/template-instance"

  def processGroupsPath(processGroupId: String) =
    "/process-groups/" + processGroupId

  def flowProcessGroupsPath(processGroupId: String) =
    "/flow/process-groups/" + processGroupId

  val flowStatusPath = "/flow/status"

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
    createProcessGroup(flowName, FlowInstance.RootProcessGroupId, clientId)
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
          template.get
        else
          throw new HttpException(ErrorConstants.DCS301.http(400))
      }

    for {
      t <- templateOrError()
      pg <- createProcessGroup(t, FlowInstance.RootProcessGroupId, clientId)
      i <- instance(t, pg, clientId)
    } yield i
  }

  def hasSubFlowExternal(flowEntity: FlowEntity): Boolean = {
    Option(flowEntity.getFlow.getProcessGroups).exists(_.size == 1) &&
      Option(flowEntity.getFlow.getProcessors).exists(_.isEmpty) &&
      Option(flowEntity.getFlow.getConnections).exists(_.size > 0)
  }

  def instance(flowTemplate: FlowTemplate, processGroup: ProcessGroup, clientId: String): Future[FlowInstance] =
    postAsJson[InstantiateTemplateRequestEntity](path = templateInstancePath(processGroup.id), body = FlowInstanceRequest(flowTemplate.getId))
      .flatMap { response =>
        val flowEntity = response.toObject[FlowEntity]
        if(hasSubFlowExternal(flowEntity))
          instance(flowEntity.getFlow.getProcessGroups.asScala.head.getComponent.getId,
            flowTemplate.name,
            flowEntity.getFlow.getConnections.asScala.map(c => ConnectionAdapter(c)).toList,
            clientId)

        else
          Future(FlowInstance(flowEntity, processGroup.id, processGroup.getName, processGroup.version))
      }



  def updatePortName(c_fi_cid: (Connection, ProcessorInstance, String)): Future[ProcessorInstance] = {
    if (c_fi_cid._2.properties(ExternalProcessorProperties.InputPortNameKey) == c_fi_cid._1.config.destination.name) {
      val inputPortName = UUID.randomUUID().toString
      ioPortApi.updateInputPortName(inputPortName,
        c_fi_cid._1.config.source.id,
        c_fi_cid._3)
        .flatMap(port =>
          processorApi.updateProperties(c_fi_cid._2.id,
            Map(
              ExternalProcessorProperties.RootInputConnectionIdKey -> c_fi_cid._1.id,
              ExternalProcessorProperties.SenderKey ->
                ExternalProcessorProperties.nifiSenderWithArgs(NifiApiConfig.BaseUiUrl, inputPortName)
            ),
            c_fi_cid._3))
    } else if(c_fi_cid._2.properties(ExternalProcessorProperties.OutputPortNameKey) == c_fi_cid._1.config.source.name) {
      val outputPortName = UUID.randomUUID().toString
      ioPortApi.updateOutputPortName(outputPortName,
        c_fi_cid._1.config.destination.id,
        c_fi_cid._3)
        .flatMap(port =>
          processorApi.updateProperties(c_fi_cid._2.id,
            Map(
              ExternalProcessorProperties.RootOutputConnectionIdKey -> c_fi_cid._1.id,
              ExternalProcessorProperties.ReceiverKey ->
                ExternalProcessorProperties.nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, outputPortName)
            ),
            c_fi_cid._3))
    } else
      Future(c_fi_cid._2)
  }

  override def instance(flowInstanceId: String,
                        flowInstanceName: String,
                        externalConnections: List[Connection],
                        clientId: String): Future[FlowInstance] = {
    val flowInstance = instance(flowInstanceId)
    flowInstance
      .flatMap { fi =>
        val cps: List[(Connection, ProcessorInstance, String)] = externalConnections
          .flatMap(c => fi.externalProcessors
            .map(p => (c, p, clientId)))
        Control.serialiseFutures(cps)(updatePortName)
          .flatMap(pis => updateName(NameId(flowInstanceName), fi.id, fi.version, clientId))
          .map(fi => FlowInstanceWithExternalConnections(fi, externalConnections))
      }
  }



  override def instance(flowInstanceId: String, clientId: String): Future[FlowInstance] = {
    instance(flowInstanceId)
      .flatMap { flowInstance =>
        if(flowInstance.hasExternalProcessors) {
          var rootConnections: List[Future[Connection]] = Nil
          flowInstance.externalProcessors
            .foreach { p =>
              p.properties.get(ExternalProcessorProperties.RootInputConnectionIdKey)
                .filter(_.trim.nonEmpty)
                .foreach(ricid => rootConnections = connectionApi.find(ricid, clientId) :: rootConnections)
              p.properties.get(ExternalProcessorProperties.RootOutputConnectionIdKey)
                .filter(_.trim.nonEmpty)
                .foreach(rocid => rootConnections = connectionApi.find(rocid, clientId) :: rootConnections)
            }
          Future.sequence(rootConnections)
            .map(cs => FlowInstanceWithExternalConnections(flowInstance, cs))
        }
        else
          Future(flowInstance)
      }
  }


  private def instance(flowInstanceId: String): Future[FlowInstance] = {
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
      getAsJson(path = flowProcessGroupsPath(FlowInstance.RootProcessGroupId))
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


  override def updateName(name: String, flowInstanceId: String, version: Long, clientId: String): Future[FlowInstance] = {
    putAsJson(path = processGroupsPath(flowInstanceId),
      body = FlowInstanceUpdateRequest(name, flowInstanceId, version, clientId))
      .map { response =>
        FlowInstance(response.toObject[ProcessGroupEntity])
      }
  }


  override def start(flowInstanceId: String, clientId: String): Future[FlowInstance] = {
    instance(flowInstanceId, clientId)
      .flatMap(fi => Future.sequence(fi.rootPortIdVersions.map(rpiv => ioPortApi.start(rpiv._1, rpiv._2, clientId)))
        .map(_.forall(identity)))
      .flatMap { _ =>
        putAsJson[FlowInstanceStartRequest](path = flowProcessGroupsPath(flowInstanceId),
          body = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateRunning))
          .map { response =>
            response.toObject[FlowInstance]
          }
      }
  }

  override def stop(flowInstanceId: String, clientId: String): Future[FlowInstance] = {
    instance(flowInstanceId, clientId)
      .flatMap(fi => Future.sequence(fi.rootPortIdVersions.map(rpiv => ioPortApi.stop(rpiv._1, rpiv._2, clientId)))
        .map(_.forall(identity)))
      .flatMap { _ =>
        putAsJson[FlowInstanceStartRequest](path = flowProcessGroupsPath(flowInstanceId),
          body = FlowInstanceStartRequest(flowInstanceId, NifiProcessorClient.StateStopped))
          .map { response =>
            response.toObject[FlowInstance]
          }
      }
  }

  private def remove(flowInstanceId: String, version: Long, clientId: String): Future[Boolean] = {

    instance(flowInstanceId)
      .map(fi => fi.connections)
      .flatMap(cs => Future.sequence(cs.map(c => connectionApi.remove(c, clientId)))
        .map(_.forall(identity)))
      .flatMap( _ =>
        deleteAsJson(path = processGroupsPath(flowInstanceId),
          queryParams = Revision.params(version, clientId))
          .map { response =>
            response != null
          }
      )
  }

  override def remove(flowInstanceId: String, version: Long, clientId: String, hasExternal: Boolean = false): Future[Boolean] = {

    if(hasExternal)
      instance(flowInstanceId, clientId).flatMap(fi =>
        remove(flowInstanceId, version, clientId, fi.externalConnections))
    else
      remove(flowInstanceId, version, clientId)
  }

  override def remove(flowInstanceId: String, version: Long, clientId: String, externalConnections: List[Connection]): Future[Boolean] = {
    Future.sequence(externalConnections.map(c => connectionApi.remove(c, clientId)))
      .map(_.forall(identity))
      .flatMap(deleteOk => if(deleteOk) remove(flowInstanceId, version, clientId) else Future(false))

  }

  def createProcessGroup(name: String, processGroupId: String, clientId: String): Future[ProcessGroup] = {
    postAsJson[ProcessGroupEntity](path = processGroupsPath(processGroupId) + "/process-groups",
      body = FlowInstanceContainerRequest(NameId(name), clientId))
      .map { response =>
        ProcessGroup(response.toObject[ProcessGroupEntity])
      }
  }

  def createProcessGroup(flowTemplate: FlowTemplate, processGroupId: String, clientId: String): Future[ProcessGroup] = {

    if(flowTemplate.hasExternal) {
      processGroup(processGroupId)
    } else
      createProcessGroup(flowTemplate.name, processGroupId, clientId)
  }

  def processGroupVersion(flowInstanceId: String): Future[String] = {
    getAsJson(path = processGroupsPath(flowInstanceId))
      .map { response =>
        response.toObject[ProcessGroupEntity].getRevision.getVersion.toString
      }
  }

  def processGroup(flowInstanceId: String): Future[ProcessGroup] = {
    getAsJson(path = processGroupsPath(flowInstanceId))
      .map { response =>
        ProcessGroup(response.toObject[ProcessGroupEntity])
      }
  }

  def status(): Future[Boolean] = {
    getAsJson(path = flowStatusPath)
      .map { _ =>
        true
      }
  }

}
