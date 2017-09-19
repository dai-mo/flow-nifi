package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.service.{FlowApiService, IOPortApiService}
import org.dcs.flow.nifi._
import org.dcs.flow.{DetailedLoggingFilter, FlowUnitSpec, IT}
import org.glassfish.jersey.filter.LoggingFilter

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

  ioPortApi.requestFilter(new LoggingFilter)
  ioPortApi.requestFilter(new DetailedLoggingFilter)

  flowApi.requestFilter(new LoggingFilter)
  flowApi.requestFilter(new DetailedLoggingFilter)

  "Input port creation / deletion" must "be valid" taggedAs IT in {
    validateInputPortCreationDeletion(ioPortApi, flowApi)
  }

  "Output port creation / deletion" must "be valid" taggedAs IT in {
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
    assert(iconn.config.source.name == iconn.config.destination.name)

    val pgInputPort = ioPortApi.inputPort(iconn.config.destination.id).futureValue
    assert(iconn.config.destination.name == pgInputPort.name)

    assert(connectionApi.removeRootInputPortConnection(iconn,
      iconn.version,
      ClientId).
      futureValue)

  }

  def validateOutputPortCreationDeletion(ioPortClient: IOPortApiService,
                                         flowClient: FlowApiService): Unit = {
    val flow = flowClient.create("OutputPortTest", ClientId).futureValue
    val oconn = ioPortClient.createOutputPort(flow.id, ClientId).futureValue
    assert(oconn.config.source.name.nonEmpty)
    assert(oconn.config.source.name == oconn.config.destination.name)

    val pgOutputPort = ioPortApi.outputPort(oconn.config.source.id).futureValue
    assert(oconn.config.source.name == pgOutputPort.name)

    assert(connectionApi.removeRootOutputPortConnection(oconn,
      oconn.version,
      ClientId).
      futureValue)


  }

}
