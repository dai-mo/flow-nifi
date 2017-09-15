package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.service.{FlowApiService, IOPortApiService}
import org.dcs.flow.{DetailedLoggingFilter, FlowUnitSpec, IT}
import org.dcs.flow.nifi._
import org.glassfish.jersey.filter.LoggingFilter

import scala.concurrent.ExecutionContext.Implicits._

object IOPortApiSpec {
  val ClientId = UUID.randomUUID().toString
}

class IOPortApiSpec {

}

class IOPortApiISpec extends IOPortApiBehaviours {
  val ioPortClient = new NifiIOPortApi

  val flowClient = new NifiFlowApi
  // flowClient.requestFilter(new LoggingFilter)
  // flowClient.requestFilter(new DetailedLoggingFilter)

  "Input port creation" must "be valid" taggedAs IT in {
    validateInputPortCreation(ioPortClient, flowClient)
  }

  "Output port creation" must "be valid" taggedAs IT in {
    validateOutputPortCreation(ioPortClient, flowClient)
  }
}

trait IOPortApiBehaviours extends FlowUnitSpec {
  import IOPortApiSpec._

  def validateInputPortCreation(ioPortClient: IOPortApiService,
                                flowClient: FlowApiService): Unit = {

    val inputPort = flowClient.create("InputPortTest", ClientId)
      .flatMap(flow => ioPortClient.createInputPort(flow.id, ClientId))
      .futureValue
    assert(inputPort.name.nonEmpty)
  }

  def validateOutputPortCreation(ioPortClient: IOPortApiService,
                                flowClient: FlowApiService): Unit = {

    val inputPort = flowClient.create("OutputPortTest", ClientId)
      .flatMap(flow => ioPortClient.createOutputPort(flow.id, ClientId))
      .futureValue
    assert(inputPort.name.nonEmpty)
  }

}
