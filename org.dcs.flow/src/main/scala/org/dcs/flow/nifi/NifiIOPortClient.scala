/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dcs.flow.nifi


import org.apache.nifi.web.api.entity.PortEntity
import org.dcs.api.service._
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient

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
                           rootPortEntityId: String,
                           portName: String): (Connectable, Connectable) = {
    val processGroupPortConnectable = Connectable(portEntityId, portType, processGroupId, name = portName)
    val rootPortConnectable = Connectable(rootPortEntityId, portType, FlowInstance.RootProcessGroupId, name = portName)

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
                         clientId: String): Future[Connection] = {
    postAsJson(path = portPath(processGroupId),
      body = FlowPortRequest(portType, clientId))
      .flatMap { response =>
        val portEntity = response.toObject[PortEntity]
        postAsJson(path = portPath(FlowInstance.RootProcessGroupId),
          body = FlowPortRequest(portType,
            clientId))
          .flatMap { response =>
            val rootPortEntity = response.toObject[PortEntity]
            val portConnectables = connectables(portType, processGroupId, portEntity.getId, rootPortEntity.getId, rootPortEntity.getComponent.getName)
            val connectionConfig = ConnectionConfig(FlowInstance.RootProcessGroupId,
              portConnectables._1,
              portConnectables._2,
              Set(),
              Set())
            connectionApi.createStdConnection(connectionConfig, clientId)
          }
      }
  }

  override def createInputPort(processGroupId: String,
                               clientId: String): Future[Connection] = {
    createPort(FlowComponent.InputPortType, inputPortsCreatePath, processGroupId, clientId)
  }

  override def createOutputPort(processGroupId: String,
                                clientId: String): Future[Connection] = {
    createPort(FlowComponent.OutputPortType, outputPortsCreatePath, processGroupId, clientId)
  }

  def updateName(portName: String,
                 portId: String,
                 portType: String,
                 portPath: (String) => String,
                 clientId: String): Future[IOPort] = {
    putAsJson(path = portPath(portId),
      body = FlowPortRequest(portId, portType, portName, Revision.InitialVersion,  clientId))
      .map { response =>
        IOPortAdapter(response.toObject[PortEntity])
      }
  }

  override def updateInputPortName(portName: String,
                                   portId: String,
                                   clientId: String): Future[IOPort] = {
    updateName(portName, portId, FlowComponent.InputPortType, inputPortsPath, clientId)
  }

  override def updateOutputPortName(portName: String,
                                    portId: String,
                                    clientId: String): Future[IOPort] = {
    updateName(portName, portId, FlowComponent.OutputPortType, outputPortsPath, clientId)
  }

  override def deleteInputPort(rootPortId: String, inputPortId: String, clientId: String): Future[Boolean] = {
    deleteInputPort(rootPortId, clientId)
      .flatMap(iport => deleteInputPort(inputPortId, clientId)
        .map(_.isDefined))
  }

  override def deleteInputPort(inputPortId: String, clientId: String): Future[Option[IOPort]] = {
    inputPort(inputPortId)
      .flatMap { iport =>
        deleteAsJson(path = inputPortsPath(inputPortId),
          queryParams = Revision.params(iport.version, clientId))
          .map { response =>
            Option(IOPortAdapter(response.toObject[PortEntity]))
          }
      }
  }

  override def deleteOutputPort(outputPortId: String, rootPortId: String, clientId: String): Future[Boolean] = {

    deleteOutputPort(outputPortId, clientId)
      .flatMap(oport => deleteOutputPort(rootPortId, clientId)
        .map(_.isDefined))

  }

  override def deleteOutputPort(outputPortId: String, clientId: String):  Future[Option[IOPort]] = {
    outputPort(outputPortId)
      .flatMap { oport =>
        deleteAsJson(path = outputPortsPath(outputPortId),
          queryParams = Revision.params(oport.version, clientId))
          .map { response =>
            Option(IOPortAdapter(response.toObject[PortEntity]))
          }
      }
  }

  def stateChange(ioPort: IOPort,
                  portPath: (String) => String,
                  state: String,
                  clientId: String): Future[Boolean] = {

    putAsJson(path = portPath(ioPort.id),
      body = IOPortStateRequest(ioPort.id, state, ioPort.version, clientId))
      .map(_.toObject[PortEntity].getComponent.getState == state)

  }

  def stateChange(portId: String, portType: String, state: String, clientId: String): Future[Boolean] = {
    portType match {
      case FlowComponent.InputPortType => inputPort(portId)
        .flatMap(port => stateChange(port, inputPortsPath _ , state, clientId))
      case FlowComponent.OutputPortType => outputPort(portId)
        .flatMap(port => stateChange(port, outputPortsPath _ , state, clientId))
      case _ => Future(false)
    }
  }

  def start(portId: String, portType: String, clientId: String): Future[Boolean] = {
    stateChange(portId, portType, NifiProcessorClient.StateRunning, clientId)
  }

  def stop(portId: String, portType: String, clientId: String): Future[Boolean] = {
    stateChange(portId, portType, NifiProcessorClient.StateStopped, clientId)
  }
}
