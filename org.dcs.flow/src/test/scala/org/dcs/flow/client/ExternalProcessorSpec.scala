package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{ExternalProcessorProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.commons.error.HttpException
import org.dcs.flow.FlowGraph.FlowGraphNode
import org.dcs.flow.nifi.{ProcessorInstance => _, _}
import org.dcs.flow._
import org.glassfish.jersey.filter.LoggingFilter
import org.scalatest.Assertion
import org.dcs.commons.serde.JsonSerializerImplicits._

import scala.concurrent.Future

object ExternalProcessorSpec {
  val ClientId: String = UUID.randomUUID().toString
  val FlowInstanceName = "ExternalProcessorTest"
  val ServiceClassPrefix = "org.dcs.core.service."
  val DataGeneratorProcessorService = "DataGeneratorProcessorService"
  val SparkBasicStatsProcessorService = "SparkBasicStatsProcessorService"
  val CSVFileOutputProcessorService = "CSVFileOutputProcessorService"

  val flowApi = new NifiFlowApi
  val processorApi = new NifiProcessorApi
  val connectionApi = new NifiConnectionApi
  val ioPortApi = new NifiIOPortApi

  flowApi.requestFilter(new LoggingFilter)
  flowApi.requestFilter(new DetailedLoggingFilter)

  processorApi.requestFilter(new LoggingFilter)
  processorApi.requestFilter(new DetailedLoggingFilter)

  connectionApi.requestFilter(new LoggingFilter)
  connectionApi.requestFilter(new DetailedLoggingFilter)

  ioPortApi.requestFilter(new LoggingFilter)
  ioPortApi.requestFilter(new DetailedLoggingFilter)

  val dgPsd = ProcessorServiceDefinition(
    ServiceClassPrefix + DataGeneratorProcessorService,
    RemoteProcessor.IngestionProcessorType,
    false)

  val sbsPsd = ProcessorServiceDefinition(
    ServiceClassPrefix + SparkBasicStatsProcessorService,
    RemoteProcessor.ExternalProcessorType,
    true)

  val csvPsd = ProcessorServiceDefinition(
    ServiceClassPrefix + CSVFileOutputProcessorService,
    RemoteProcessor.SinkProcessorType,
    true)
}

class ExternalProcessorSpec extends ExternalProcessorBehaviour {

}

class ExternalProcessorISpec extends ExternalProcessorBehaviour {
  import ExternalProcessorSpec._

  "Creation / Deletion of Connections to / from an external processor" should "be valid" taggedAs IT in {
    val flowInstance = flowApi.create(FlowInstanceName, ClientId).futureValue
    val dgP = processorApi.create(dgPsd, flowInstance.id, ClientId).futureValue
    val sbsP = processorApi.create(sbsPsd, flowInstance.id, ClientId).futureValue
    val csvP = processorApi.create(csvPsd, flowInstance.id, ClientId).futureValue

    val dgPToSbsPConnectionConfig = ConnectionConfig(
      flowInstance.id,
      Connectable(dgP.id, FlowComponent.ProcessorType, flowInstance.id),
      Connectable(sbsP.id, FlowComponent.ExternalProcessorType, flowInstance.id),
      Set("success"),
      Set("failure")
    )

    val outputPortConnection = validateCreateConnectionToExternalProcessor(connectionApi,
      ioPortApi,
      processorApi,
      dgPToSbsPConnectionConfig,
      dgP.id,
      sbsP.id)

    val sbsPToCsvPConnectionConfig = ConnectionConfig(
      flowInstance.id,
      Connectable(sbsP.id, FlowComponent.ExternalProcessorType, flowInstance.id),
      Connectable(csvP.id, FlowComponent.ProcessorType, flowInstance.id)
    )

    val inputPortConnection = validateCreateConnectionFromExternalProcessor(connectionApi,
      ioPortApi,
      processorApi,
      sbsPToCsvPConnectionConfig,
      csvP.id,
      sbsP.id)

    val version = flowApi.instance(flowInstance.id).futureValue.version

    val dgPToSbsPConnection =
      Connection("", "", version, dgPToSbsPConnectionConfig, "", "", -1, List(), Set(outputPortConnection))
    validateRemoveConnectionToExternalProcessor(connectionApi, ioPortApi, dgPToSbsPConnection)

    val sbsPToCsvPConnection =
      Connection("", "", version, sbsPToCsvPConnectionConfig, "", "", -1, List(), Set(inputPortConnection))
    validateRemoveConnectionFromExternalProcessor(connectionApi, ioPortApi, sbsPToCsvPConnection)

    flowApi.remove(flowInstance.id, version, ClientId).map(deleteOk => assert(deleteOk))
  }

  "Creation / Deletion of FlowInstance with an external processor" should "be valid" taggedAs IT in {
    var flowInstance = flowApi.create (FlowInstanceName, ClientId).futureValue
    val dgP = processorApi.create (dgPsd, flowInstance.id, ClientId).futureValue
    val sbsP = processorApi.create (sbsPsd, flowInstance.id, ClientId).futureValue
    val csvP = processorApi.create (csvPsd, flowInstance.id, ClientId).futureValue

    val dgPToSbsPConnectionConfig = ConnectionConfig (
      flowInstance.id,
      Connectable (dgP.id, FlowComponent.ProcessorType, flowInstance.id),
      Connectable (sbsP.id, FlowComponent.ExternalProcessorType, flowInstance.id),
      Set ("success"),
      Set ("failure")
    )

    val outputPortConnection = validateCreateConnectionToExternalProcessor (connectionApi,
      ioPortApi,
      processorApi,
      dgPToSbsPConnectionConfig,
      dgP.id,
      sbsP.id)

    val sbsPToCsvPConnectionConfig = ConnectionConfig (
      flowInstance.id,
      Connectable (sbsP.id, FlowComponent.ExternalProcessorType, flowInstance.id),
      Connectable (csvP.id, FlowComponent.ProcessorType, flowInstance.id)
    )
    validateCreateConnectionFromExternalProcessor (connectionApi,
      ioPortApi,
      processorApi,
      sbsPToCsvPConnectionConfig,
      csvP.id,
      sbsP.id)

    flowApi.instance(flowInstance.id)
      .map { fi =>
        validateFlowInstanceWithExternalProcessor(flowApi, fi)
      }
      .flatMap { fi =>
        flowApi.remove(fi.id,
          fi.version,
          ClientId,
          fi.connections.filter(c =>
            c.config.source.componentType == FlowComponent.ExternalProcessorType ||
              c.config.destination.componentType == FlowComponent.ExternalProcessorType))
          .map(deleteOk => assert(deleteOk))
      }

  }

}



trait ExternalProcessorBehaviour extends AsyncFlowUnitSpec {
  import ExternalProcessorSpec._

  def validateCreateConnectionToExternalProcessor(connectionApi: ConnectionApiService,
                                                  ioPortApi: IOPortApiService,
                                                  processorApi: ProcessorApiService,
                                                  connectionConfig: ConnectionConfig,
                                                  sourceProcessorId: String,
                                                  externalProcessorId: String): Connection = {
    val connection = connectionApi.create(connectionConfig, ClientId).futureValue

    val outputPort = ioPortApi.outputPort(connection.config.destination.id).futureValue

    val receiverArgs = ExternalProcessorProperties
      .nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, outputPort.name)

    val externalProcessor = processorApi.instance(externalProcessorId).futureValue

    assert(externalProcessor.properties(ExternalProcessorProperties.ReceiverKey) == receiverArgs)
    assert(externalProcessor.properties(ExternalProcessorProperties.RootOutputConnectionKey).toObject[Connection].id ==
      connection.relatedConnections.head.id)

    assert(connection.config.source.componentType == FlowComponent.ProcessorType)
    assert(connection.config.destination.componentType == FlowComponent.OutputPortType)
    connection
  }

  def validateCreateConnectionFromExternalProcessor(connectionApi: ConnectionApiService,
                                                    ioPortApi: IOPortApiService,
                                                    processorApi: ProcessorApiService,
                                                    connectionConfig: ConnectionConfig,
                                                    destinationProcessorId: String,
                                                    externalProcessorId: String): Connection = {
    val connection = connectionApi.create(connectionConfig, ClientId).futureValue

    val inputPort = ioPortApi.inputPort(connection.config.source.id).futureValue

    val senderArgs = ExternalProcessorProperties
      .nifiSenderWithArgs(NifiApiConfig.BaseUiUrl, inputPort.name)

    val externalProcessor = processorApi.instance(externalProcessorId).futureValue

    assert(externalProcessor.properties(ExternalProcessorProperties.SenderKey) == senderArgs)
    assert(externalProcessor.properties(ExternalProcessorProperties.RootInputConnectionKey).toObject[Connection].id ==
      connection.relatedConnections.head.id)

    assert(connection.config.source.componentType == FlowComponent.InputPortType)
    assert(connection.config.destination.componentType == FlowComponent.ProcessorType)
    connection
  }

  def validateRemoveConnectionFromExternalProcessor(connectionApi: ConnectionApiService,
                                                    ioPortApi: IOPortApiService,
                                                    connection: Connection): Future[Assertion] = {
    connectionApi.remove(connection, connection.version, ClientId).futureValue
    val rootConnection = connection.relatedConnections.head.relatedConnections.head

    recoverToExceptionIf[HttpException] {
      ioPortApi.inputPort(rootConnection.config.source.id)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))

    recoverToExceptionIf[HttpException] {
      ioPortApi.inputPort(rootConnection.config.destination.id)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))
  }

  def validateRemoveConnectionToExternalProcessor(connectionApi: ConnectionApiService,
                                                  ioPortApi: IOPortApiService,
                                                  connection: Connection): Future[Assertion] = {
    connectionApi.remove(connection, connection.version, ClientId).futureValue
    val rootConnection = connection.relatedConnections.head.relatedConnections.head

    recoverToExceptionIf[HttpException] {
      ioPortApi.outputPort(rootConnection.config.source.id)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))

    recoverToExceptionIf[HttpException] {
      ioPortApi.outputPort(rootConnection.config.destination.id)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))
  }


  def validateFlowInstanceWithExternalProcessor(flowApi: FlowApiService,
                                                flowInstance: FlowInstance): FlowInstance = {
    assert(flowInstance.connections.size == 2)
    flowInstance
  }


}
