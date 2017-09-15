package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.service.{FlowApiService, IOPortApiService}
import org.dcs.flow.nifi._
import org.dcs.flow.{FlowUnitSpec, IT}

object IOPortApiSpec {
  val ClientId = UUID.randomUUID().toString

  val ioPortApi = new NifiIOPortApi
  val flowClient = new NifiFlowApi
}

class IOPortApiSpec {

}

class IOPortApiISpec extends IOPortApiBehaviours {
  import IOPortApiSpec._

  // flowClient.requestFilter(new LoggingFilter)
  // flowClient.requestFilter(new DetailedLoggingFilter)

  "Input port creation" must "be valid" taggedAs IT in {
    validateInputPortCreation(ioPortApi, flowClient)
  }

  "Output port creation" must "be valid" taggedAs IT in {
    validateOutputPortCreation(ioPortApi, flowClient)
  }
}

trait IOPortApiBehaviours extends FlowUnitSpec {
  import IOPortApiSpec._

  def validateInputPortCreation(ioPortClient: IOPortApiService,
                                flowClient: FlowApiService): Unit = {

    val flow = flowClient.create("InputPortTest", ClientId).futureValue
    val iportconn = ioPortClient.createInputPort(flow.id, ClientId).futureValue
    assert(iportconn._1.name.nonEmpty)
    assert(iportconn._1.id == iportconn._2.config.source.id)

    val pgInputPort = ioPortApi.inputPort(iportconn._2.config.destination.id).futureValue
    assert(iportconn._1.id != pgInputPort.id)
    assert(iportconn._1.name == pgInputPort.name)

  }

  def validateOutputPortCreation(ioPortClient: IOPortApiService,
                                flowClient: FlowApiService): Unit = {
    val flow = flowClient.create("OutputPortTest", ClientId).futureValue
    val iportconn = ioPortClient.createOutputPort(flow.id, ClientId).futureValue
    assert(iportconn._1.name.nonEmpty)
    assert(iportconn._1.id == iportconn._2.config.destination.id)

    val pgOutputPort = ioPortApi.outputPort(iportconn._2.config.source.id).futureValue
    assert(iportconn._1.id != pgOutputPort.id)
    assert(iportconn._1.name == pgOutputPort.name)
  }

}
