package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{ExternalProcessorProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.flow.{FlowUnitSpec, IT}
import org.dcs.flow.nifi.{ProcessorInstance => _, _}

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

  "Creation of Flow Instance with an external processor" should "be valid" taggedAs IT in {
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
    validateConnectionToExternalProcessor(connectionApi,
      ioPortApi,
      processorApi,
      dgPToSbsPConnectionConfig,
      dgP.id,
      sbsP.id)
  }

}

trait ExternalProcessorBehaviour extends FlowUnitSpec {
  import ExternalProcessorSpec._

  def validateConnectionToExternalProcessor(connectionApi: ConnectionApiService,
                                            ioPortApi: IOPortApiService,
                                            processorApi: ProcessorApiService,
                                            connectionConfig: ConnectionConfig,
                                            sourceProcessorId: String,
                                            externalProcessorId: String): Unit = {
    val connection = connectionApi.create(connectionConfig, ClientId).futureValue

    val outputPort = ioPortApi.outputPort(connection.config.destination.id).futureValue

    val receiverArgs = ExternalProcessorProperties
      .nifiReceiverWithArgs(NifiApiConfig.BaseUiUrl, outputPort.name)

    val externalProcessor = processorApi.instance(externalProcessorId).futureValue

    assert(externalProcessor.properties(ExternalProcessorProperties.ReceiverKey) == receiverArgs)

    assert(connection.config.source.id == sourceProcessorId)
    assert(connection.config.destination.id == outputPort.id)
  }

}
