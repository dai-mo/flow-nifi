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
    val iportconn = ioPortClient.createInputPort(flow.id, ClientId).futureValue
    assert(iportconn._1.name.nonEmpty)
    assert(iportconn._1.id == iportconn._2.config.source.id)

    val pgInputPort = ioPortApi.inputPort(iportconn._2.config.destination.id).futureValue
    assert(iportconn._1.id != pgInputPort.id)
    assert(iportconn._1.name == pgInputPort.name)

    assert(connectionApi.removeRootInputPortConnection(iportconn._2,
      iportconn._2.version,
      ClientId).
      futureValue)

  }

  def validateOutputPortCreationDeletion(ioPortClient: IOPortApiService,
                                         flowClient: FlowApiService): Unit = {
    val flow = flowClient.create("OutputPortTest", ClientId).futureValue
    val oportconn = ioPortClient.createOutputPort(flow.id, ClientId).futureValue
    assert(oportconn._1.name.nonEmpty)
    assert(oportconn._1.id == oportconn._2.config.destination.id)

    val pgOutputPort = ioPortApi.outputPort(oportconn._2.config.source.id).futureValue
    assert(oportconn._1.id != pgOutputPort.id)
    assert(oportconn._1.name == pgOutputPort.name)

    assert(connectionApi.removeRootOutputPortConnection(oportconn._2,
      oportconn._2.version,
      ClientId).
      futureValue)


  }

}
