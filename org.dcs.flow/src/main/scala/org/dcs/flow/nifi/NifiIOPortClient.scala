package org.dcs.flow.nifi


import org.apache.nifi.web.api.entity.PortEntity
import org.dcs.api.service._
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.dcs.flow.nifi.internal.ProcessGroupHelper

import scala.beans.BeanProperty
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

class NifiIOPortApi extends NifiIOPortClient with NifiApiConfig

object NifiIOPortClient {

  val connectionApi = new NifiConnectionApi

  def inputPortsCreatePath(processGroupId: String): String =
    "/process-groups/" + processGroupId + "/input-ports"

  def outputPortsCreatePath(processGroupId: String): String =
    "/process-groups/" + processGroupId + "/output-ports"

  def inputPortsPath(inputPortId: String): String =
    "input-ports/" + inputPortId

  def outputPortsPath(outputPortId: String): String =
    "output-ports/" + outputPortId
}


trait NifiIOPortClient extends IOPortApiService with JerseyRestClient {
  import NifiIOPortClient._


  private def connectables(portType: String,
                           processGroupId: String,
                           portEntityId: String,
                           rootPortEntityId: String): (Connectable, Connectable) = {
    val processGroupPortConnectable = Connectable(portEntityId, portType, processGroupId)
    val rootPortConnectable = Connectable(rootPortEntityId, portType, ProcessGroupHelper.RootProcessGroupId)

    portType match {
      case FlowComponent.InputPortType => (rootPortConnectable, processGroupPortConnectable)
      case FlowComponent.OutputPortType => (processGroupPortConnectable, rootPortConnectable)
    }
  }

  override def inputPort(id: String): Future[IOPort] =
    getAsJson(path = inputPortsPath(id))
      .map(response => IOPortAdapter(response.toObject[PortEntity]))

  override def outputPort(id: String): Future[IOPort] =
    getAsJson(path = outputPortsPath(id))
      .map(response => IOPortAdapter(response.toObject[PortEntity]))

  private def createPort(portType: String,
                         portPath: (String) => String,
                         processGroupId: String,
                         clientId: String): Future[(IOPort, Connection)] = {
    postAsJson(path = portPath(processGroupId),
      body = FlowPortRequest(portType, clientId))
      .flatMap { response =>
        val portEntity = response.toObject[PortEntity]
        postAsJson(path = portPath(ProcessGroupHelper.RootProcessGroupId),
          body = FlowPortRequest(portType,
            portEntity.getComponent.getName,
            clientId))
          .flatMap { response =>
            val rootPortEntity = response.toObject[PortEntity]
            val portConnectables = connectables(portType, processGroupId, portEntity.getId, rootPortEntity.getId)
            val connectionConfig = ConnectionConfig(ProcessGroupHelper.RootProcessGroupId,
              portConnectables._1,
              portConnectables._2,
              Set(),
              Set())
            connectionApi.createStdConnection(connectionConfig, clientId)
              .map(connection => (IOPortAdapter(rootPortEntity), connection))
          }
      }
  }

  override def createInputPort(processGroupId: String,
                               clientId: String): Future[(IOPort, Connection)] = {
    createPort(FlowComponent.InputPortType, inputPortsCreatePath, processGroupId, clientId)
  }

  override def createOutputPort(processGroupId: String,
                                clientId: String): Future[(IOPort, Connection)] = {
    createPort(FlowComponent.OutputPortType, outputPortsCreatePath, processGroupId, clientId)
  }
}
