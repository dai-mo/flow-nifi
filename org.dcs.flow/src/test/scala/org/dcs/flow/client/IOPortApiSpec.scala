package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.service.{FlowApiService, IOPortApiService}
import org.dcs.flow.nifi._
import org.dcs.flow.{FlowUnitSpec, IT}

import scala.concurrent.blocking

object IOPortApiSpec {
  val ClientId = UUID.randomUUID().toString

  val ioPortApi = new NifiIOPortApi
  val flowApi = new NifiFlowApi
  val connectionApi = new NifiConnectionApi
}

class IOPortApiSpec {

}

class IOPortApiISpec extends IOPortApiBehaviours {
  import IOPortApiSpec._
  "Input port creation / update / deletion" must "be valid" taggedAs IT in {
    validateInputPortCreationDeletion(ioPortApi, flowApi)
  }

  "Output port creation / update / deletion" must "be valid" taggedAs IT in {
    validateOutputPortCreationDeletion(ioPortApi, flowApi)
  }
}

trait IOPortApiBehaviours extends FlowUnitSpec {
  import IOPortApiSpec._

  def validateInputPortCreationDeletion(ioPortClient: IOPortApiService,
                                        flowClient: FlowApiService): Unit = {

    var flow = flowClient.create("InputPortTest", ClientId).futureValue
    val iconn = ioPortClient.createInputPort(flow.id, ClientId).futureValue
    assert(iconn.config.source.name.nonEmpty)

    val rootInputPort = ioPortApi.inputPort(iconn.config.source.id).futureValue
    val pgInputPort = ioPortApi.inputPort(iconn.config.destination.id).futureValue
    assert(iconn.config.destination.name == pgInputPort.name)

    val inputPortName = "iportname"
    val updatedPgInputPort =
      ioPortApi.updateInputPortName(inputPortName,  pgInputPort.id, ClientId).futureValue
    assert(updatedPgInputPort.name == inputPortName)

    assert(ioPortApi.start(rootInputPort.id, rootInputPort.`type`, ClientId).futureValue)
    assert(ioPortApi.stop(rootInputPort.id, rootInputPort.`type`, ClientId).futureValue)

    blocking { Thread.sleep(2000) }

    assert(connectionApi.removeRootInputPortConnection(iconn,
      ClientId).
      futureValue)

    val currentFlow = flowClient.instance(flow.id, ClientId).futureValue

    flowClient.remove(currentFlow.id, currentFlow.version, ClientId)

  }

  def validateOutputPortCreationDeletion(ioPortClient: IOPortApiService,
                                         flowClient: FlowApiService): Unit = {
    val flow = flowClient.create("OutputPortTest", ClientId).futureValue
    val oconn = ioPortClient.createOutputPort(flow.id, ClientId).futureValue
    assert(oconn.config.source.name.nonEmpty)

    val rootOutputPort = ioPortApi.outputPort(oconn.config.destination.id).futureValue
    val pgOutputPort = ioPortApi.outputPort(oconn.config.source.id).futureValue
    assert(oconn.config.source.name == pgOutputPort.name)

    val outputPortName = "oportname"
    val updatedPgOutputPort =
      ioPortApi.updateOutputPortName(outputPortName,  pgOutputPort.id, ClientId).futureValue
    assert(updatedPgOutputPort.name == outputPortName)

    assert(ioPortApi.start(rootOutputPort.id, rootOutputPort.`type`, ClientId).futureValue)
    assert(ioPortApi.stop(rootOutputPort.id, rootOutputPort.`type`, ClientId).futureValue)

    blocking { Thread.sleep(2000) }

    assert(connectionApi.removeRootOutputPortConnection(oconn,
      ClientId).
      futureValue)

    val currentFlow = flowClient.instance(flow.id, ClientId).futureValue

    flowClient.remove(currentFlow.id, currentFlow.version, ClientId)
  }

}
