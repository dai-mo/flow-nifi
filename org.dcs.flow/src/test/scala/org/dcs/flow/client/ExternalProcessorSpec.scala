package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{CoreProperties, ExternalProcessorProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.commons.error.HttpException
import org.dcs.flow._
import org.dcs.flow.nifi.{ProcessorInstanceAdapter => _, _}
import org.glassfish.jersey.filter.LoggingFilter
import org.scalatest.Assertion

import scala.concurrent.Future

object ExternalProcessorSpec {
  val ClientId: String = UUID.randomUUID().toString
  val FlowInstanceName = "ExternalProcessorTest"
  val ServiceClassPrefix = "org.dcs.core.service."
  val DataGeneratorProcessorService = "DataGeneratorProcessorService"
  val SparkBasicStatsProcessorService = "SparkBasicStatsProcessorService"
  val CSVFileOutputProcessorService = "CSVFileOutputProcessorService"

  val ReadSchemaId = "org.dcs.test.Person"
  val FlowTemplateName = "SparkStatsTest"

  val flowApi = new NifiFlowApi
  val processorApi = new NifiProcessorApi
  val connectionApi = new NifiConnectionApi
  val ioPortApi = new NifiIOPortApi

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
      sbsP.id,
      ReadSchemaId)

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
      sbsP.id,
      ReadSchemaId)

    val updatedFlowInstance = flowApi.instance(flowInstance.id, ClientId).futureValue

    validateFlowInstanceWithExternalProcessor(flowApi, updatedFlowInstance, flowInstance.name)

    val version = updatedFlowInstance.version

    val dgPToSbsPConnection =
      Connection("", "", version, dgPToSbsPConnectionConfig, "", "", -1, List(), Set(outputPortConnection))
    validateRemoveConnectionToExternalProcessor(connectionApi, ioPortApi, dgPToSbsPConnection)

    val sbsPToCsvPConnection =
      Connection("", "", version, sbsPToCsvPConnectionConfig, "", "", -1, List(), Set(inputPortConnection))
    validateRemoveConnectionFromExternalProcessor(connectionApi, ioPortApi, sbsPToCsvPConnection)

    flowApi.remove(flowInstance.id, version, ClientId).map(deleteOk => assert(deleteOk))
  }

  "Instantiation / Deletion of FlowInstance with an external processor" should "be valid" taggedAs IT in {
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

    validateCreateConnectionToExternalProcessor (connectionApi,
      ioPortApi,
      processorApi,
      dgPToSbsPConnectionConfig,
      dgP.id,
      sbsP.id,
      ReadSchemaId)

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
      sbsP.id,
      ReadSchemaId)

    flowApi.instance(flowInstance.id, ClientId)
      .map { fi =>
        validateFlowInstanceWithExternalProcessor(flowApi, fi, fi.name)
      }
      .flatMap { fi =>
        flowApi.remove(fi.id,
          fi.version,
          ClientId,
          fi.externalConnections)
          .map(deleteOk => assert(deleteOk))
      }
  }

  "Deletion of an external processor" should "be valid" taggedAs IT in {
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
    validateCreateConnectionToExternalProcessor (connectionApi,
      ioPortApi,
      processorApi,
      dgPToSbsPConnectionConfig,
      dgP.id,
      sbsP.id,
      ReadSchemaId)

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
      sbsP.id,
      ReadSchemaId)

    validateRemoveExternalProcessor(processorApi, sbsP.id, flowInstance.id, sbsP.processorType, sbsP.version)
      .flatMap { a =>
        flowApi.remove(flowInstance.id,
          flowInstance.version,
          ClientId,
          flowInstance.externalConnections)
          .map(deleteOk =>  assert(deleteOk))
      }

  }

  "Instantiation of Flow with an external processor" should "be valid" taggedAs IT in {
    val flowTemplate = flowApi.templates().futureValue.find(_.name == FlowTemplateName).get
    val flowInstance = flowApi.instantiate(flowTemplate.id, ClientId).futureValue
    validateFlowInstanceWithExternalProcessor(flowApi, flowInstance, flowTemplate.name)
    flowApi.remove(flowInstance.id, flowInstance.version, ClientId, true).map(deleteOk => assert(deleteOk))
  }

  object NifiFlowApiBehaviours extends FlowApiBehaviors

  "Run of Flow with an external processor" should "be valid" taggedAs IT in {
    val flowTemplate = flowApi.templates().futureValue.find(_.name == FlowTemplateName).get
    NifiFlowApiBehaviours.validateRun(flowApi, flowTemplate.id, true)
    Future(assert(true))
  }

}



trait ExternalProcessorBehaviour extends AsyncFlowUnitSpec {
  import ExternalProcessorSpec._

  def validateCreateConnectionToExternalProcessor(connectionApi: ConnectionApiService,
                                                  ioPortApi: IOPortApiService,
                                                  processorApi: ProcessorApiService,
                                                  connectionConfig: ConnectionConfig,
                                                  sourceProcessorId: String,
                                                  externalProcessorId: String,
                                                  readSchemaId: String): Connection = {
    val connection = connectionApi.create(connectionConfig, ClientId).futureValue

    val rootOutputPort = ioPortApi.outputPort(connection.relatedConnections.head.config.destination.id).futureValue
    val flowOutputPort = ioPortApi.outputPort(connection.config.destination.id).futureValue

    val receiverArgs = ExternalProcessorProperties
      .nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, rootOutputPort.name)

    val externalProcessor = processorApi.instance(externalProcessorId).futureValue

    assert(externalProcessor.properties(ExternalProcessorProperties.ReceiverKey) == receiverArgs)
    assert(externalProcessor.properties(ExternalProcessorProperties.RootOutputConnectionIdKey) == connection.relatedConnections.head.id)
    assert(externalProcessor.properties(ExternalProcessorProperties.OutputPortNameKey) == flowOutputPort.name)

    assert(connection.config.source.componentType == FlowComponent.ProcessorType)
    assert(connection.config.destination.componentType == FlowComponent.OutputPortType)

    assert(externalProcessor.properties(CoreProperties.ReadSchemaIdKey) == readSchemaId)
    connection
  }

  def validateCreateConnectionFromExternalProcessor(connectionApi: ConnectionApiService,
                                                    ioPortApi: IOPortApiService,
                                                    processorApi: ProcessorApiService,
                                                    connectionConfig: ConnectionConfig,
                                                    destinationProcessorId: String,
                                                    externalProcessorId: String,
                                                    readSchemaId: String): Connection = {
    val connection = connectionApi.create(connectionConfig, ClientId).futureValue

    val rootInputPort = ioPortApi.inputPort(connection.relatedConnections.head.config.source.id).futureValue
    val flowInputPort = ioPortApi.inputPort(connection.config.source.id).futureValue

    val senderArgs = ExternalProcessorProperties
      .nifiSenderWithArgs(NifiApiConfig.BaseUiUrl, rootInputPort.name)

    val externalProcessor = processorApi.instance(externalProcessorId).futureValue

    assert(externalProcessor.properties(ExternalProcessorProperties.SenderKey) == senderArgs)
    assert(externalProcessor.properties(ExternalProcessorProperties.RootInputConnectionIdKey) == connection.relatedConnections.head.id)
    assert(externalProcessor.properties(ExternalProcessorProperties.InputPortNameKey) == flowInputPort.name)

    assert(connection.config.source.componentType == FlowComponent.InputPortType)
    assert(connection.config.destination.componentType == FlowComponent.ProcessorType)

    val destinationProcessor = processorApi.instance(externalProcessorId).futureValue
    assert(destinationProcessor.properties(CoreProperties.ReadSchemaIdKey) == readSchemaId)
    connection
  }

  def validateRemoveConnectionFromExternalProcessor(connectionApi: ConnectionApiService,
                                                    ioPortApi: IOPortApiService,
                                                    connection: Connection): Future[Assertion] = {
    connectionApi.remove(connection, ClientId).futureValue
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
    connectionApi.remove(connection, ClientId).futureValue
    val rootConnection = connection.relatedConnections.head.relatedConnections.head

    recoverToExceptionIf[HttpException] {
      ioPortApi.outputPort(rootConnection.config.source.id)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))

    recoverToExceptionIf[HttpException] {
      ioPortApi.outputPort(rootConnection.config.destination.id)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))
  }


  def validateFlowInstanceWithExternalProcessor(flowApi: FlowApiService,
                                                flowInstance: FlowInstance,
                                                flowName: String): FlowInstance = {
    val externalProcessor =
      flowInstance.processors.find(_.processorType == RemoteProcessor.ExternalProcessorType).get
    assert(flowInstance.name == flowName)

    assert(flowInstance.connections.size == 2)

    val toExternalProcessorConnection =
      flowInstance.connections
        .find(c => c.config.destination.componentType == FlowComponent.ExternalProcessorType).get
    val tepcDestination = toExternalProcessorConnection.config.destination
    assert(tepcDestination.id == externalProcessor.id)
    assert(tepcDestination.flowInstanceId == flowInstance.id)
    val flowOutputConnection =
      toExternalProcessorConnection.relatedConnections.head
    assert(flowOutputConnection.config.source.id == toExternalProcessorConnection.config.source.id)
    assert(flowOutputConnection.config.destination.componentType == FlowComponent.OutputPortType)
    val rootOutputConnection =
      flowOutputConnection.relatedConnections.head
    assert(rootOutputConnection.config.source.componentType == FlowComponent.OutputPortType)
    assert(rootOutputConnection.config.destination.componentType == FlowComponent.OutputPortType)
    assert(rootOutputConnection.config.source.id == flowOutputConnection.config.destination.id)
    assert(rootOutputConnection.config.source.name == flowOutputConnection.config.destination.name)
    assert(rootOutputConnection.config.source.name == externalProcessor.properties(ExternalProcessorProperties.OutputPortNameKey))

    val fromExternalProcessorConnection =
      flowInstance.connections
        .find(c => c.config.source.componentType == FlowComponent.ExternalProcessorType).get
    val fepcSource = fromExternalProcessorConnection.config.source
    assert(fepcSource.id == externalProcessor.id)
    assert(fepcSource.flowInstanceId == flowInstance.id)
    val flowInputConnection =
      fromExternalProcessorConnection.relatedConnections.head
    assert(flowInputConnection.config.destination.id == fromExternalProcessorConnection.config.destination.id)
    assert(flowInputConnection.config.source.componentType == FlowComponent.InputPortType)
    val rootInputConnection =
      flowInputConnection.relatedConnections.head
    assert(rootInputConnection.config.source.componentType == FlowComponent.InputPortType)
    assert(rootInputConnection.config.destination.componentType == FlowComponent.InputPortType)
    assert(rootInputConnection.config.destination.id == flowInputConnection.config.source.id)
    assert(rootInputConnection.config.destination.name == flowInputConnection.config.source.name)
    assert(rootInputConnection.config.destination.name == externalProcessor.properties(ExternalProcessorProperties.InputPortNameKey))

    flowInstance
  }

  def validateRemoveExternalProcessor(processorApi: ProcessorApiService,
                                      processorId: String,
                                      flowInstanceId: String,
                                      processorType: String,
                                      version: Long): Future[Assertion] = {

    assert(processorApi.remove(processorId,
      flowInstanceId,
      processorType,
      version,
      ClientId).futureValue)

    recoverToExceptionIf[HttpException] {
      processorApi.instance(processorId)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))

  }


}
