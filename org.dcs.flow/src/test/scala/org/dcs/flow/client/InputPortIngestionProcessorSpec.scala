package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{ExternalProcessorProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.commons.error.HttpException
import org.dcs.flow.nifi.{NifiFlowApi, NifiIOPortApi, NifiProcessorApi}
import org.dcs.flow.{AsyncFlowUnitSpec, IT}
import org.scalatest.Assertion

import scala.concurrent.Future

object InputPortIngestionSpec {
  val ClientId: String = UUID.randomUUID().toString
  val FlowInstanceName = "InputPortIngestionProcessorTest"
  val ServiceClassPrefix = "org.dcs.core.service."
  val KaaIngestionProcessorService = "KaaIngestionProcessorService"

  val FlowTemplateName = "KaaTest"

  val kaaPsd = ProcessorServiceDefinition(
    ServiceClassPrefix + KaaIngestionProcessorService,
    RemoteProcessor.InputPortIngestionType,
    false)

  val flowApi = new NifiFlowApi
  val processorApi = new NifiProcessorApi
  val ioPortApi = new NifiIOPortApi
}

class InputPortIngestionProcessorSpec extends InputPortIngestionBehaviour {

}

class InputPortIngestionProcessorISpec extends InputPortIngestionBehaviour {
  import InputPortIngestionSpec._

  "Creation / Deletion of Input Ingestion Processor" should "be valid" taggedAs IT in {
    var flowInstance = flowApi.create(FlowInstanceName, ClientId).futureValue
    var processor = validateCreateInputPortIngestionProcessor(processorApi, kaaPsd, flowInstance.id)
    flowInstance = validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, FlowInstanceName)
    processor = processorApi.instance(processor.id).futureValue
    validateDeleteInputPortIngestionProcessor(processorApi,
      ioPortApi,
      processor.id,
      flowInstance.id,
      processor.version,
      flowInstance.connections.head.config.source.id,
      flowInstance.connections.head.relatedConnections.head.config.source.id)
    flowApi.remove(flowInstance.id, flowInstance.version, ClientId).futureValue
    succeed
  }

  "Deletion of Flow Instance with  Input Ingestion Processor" should "be valid" taggedAs IT in {
    var flowInstance = flowApi.create(FlowInstanceName, ClientId).futureValue
    var processor = validateCreateInputPortIngestionProcessor(processorApi, kaaPsd, flowInstance.id)
    flowInstance = validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, FlowInstanceName)
    processor = processorApi.instance(processor.id).futureValue

    flowApi.remove(flowInstance.id, flowInstance.version, ClientId, hasExternal = true)
      .map(deleteOk => assert(deleteOk))

  }

  "Instantiation of Flow with an Input Ingestion Processor" should "be valid" taggedAs IT in {
    val flowTemplate = flowApi.templates().futureValue.find(_.name == FlowTemplateName).get
    val flowInstance = flowApi.instantiate(flowTemplate.id, ClientId).futureValue
    validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, FlowTemplateName)
    flowApi.remove(flowInstance.id, flowInstance.version, ClientId, true).map(deleteOk => assert(deleteOk))
  }
}

trait InputPortIngestionBehaviour extends AsyncFlowUnitSpec {
  import InputPortIngestionSpec._

  def validateCreateInputPortIngestionProcessor(processorApi: ProcessorApiService,
                                                psd: ProcessorServiceDefinition,
                                                flowInstanceId: String): ProcessorInstance = {
    val processor = processorApi.create(psd, flowInstanceId, ClientId).futureValue
    assert(processor.processorType == RemoteProcessor.InputPortIngestionType)
    processor
  }

  def validateFlowInstanceWithInputPortIngestionProcessor(flowApi: FlowApiService,
                                                          flowInstanceId: String,
                                                          flowName: String): FlowInstance = {
    val flowInstance = flowApi.instance(flowInstanceId, ClientId).futureValue
    assert(flowInstance.name == flowName)
    val inputPortIngestionProcessor =
      flowInstance.processors.find(_.processorType == RemoteProcessor.InputPortIngestionType).get
    assert(flowInstance.connections.size == 1)
    val inputPortConnection = flowInstance.connections.head
    assert(inputPortConnection.config.source.componentType == FlowComponent.InputPortType)
    assert(inputPortConnection.config.destination.componentType == FlowComponent.InputPortIngestionType)

    assert(inputPortConnection.relatedConnections.size == 1)
    val rootPortConnection = inputPortConnection.relatedConnections.head
    assert(rootPortConnection.config.source.componentType == FlowComponent.InputPortType)
    assert(rootPortConnection.config.destination.componentType == FlowComponent.InputPortType)

    assert(rootPortConnection.id ==
      inputPortIngestionProcessor.properties(ExternalProcessorProperties.RootInputConnectionIdKey))
    assert(inputPortConnection.config.source.name ==
    inputPortIngestionProcessor.properties(ExternalProcessorProperties.InputPortNameKey))
    flowInstance
  }

  def validateDeleteInputPortIngestionProcessor(processorApi: ProcessorApiService,
                                                ioPortApi: IOPortApiService,
                                                processorId: String,
                                                flowInstanceId: String,
                                                version: Long,
                                                flowInputPortId: String,
                                                rootInputPortId: String): Unit = {
    val deleteoK = processorApi.remove(processorId, flowInstanceId, RemoteProcessor.InputPortIngestionType, version, ClientId).futureValue
    assert(deleteoK)
    recoverToExceptionIf[HttpException] {
      processorApi.instance(processorId)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))
    recoverToExceptionIf[HttpException] {
      ioPortApi.inputPort(flowInputPortId)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))
    recoverToExceptionIf[HttpException] {
      ioPortApi.inputPort(rootInputPortId)
    }.map(ex => assert(ex.errorResponse.httpStatusCode == 404))

  }

}