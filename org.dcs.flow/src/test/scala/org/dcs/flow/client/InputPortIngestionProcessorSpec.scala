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

package org.dcs.flow.client

import java.util.UUID

import org.dcs.api.processor.{ExternalProcessorProperties, RemoteProcessor}
import org.dcs.api.service._
import org.dcs.commons.error.HttpException
import org.dcs.flow.nifi.{NifiFlowApi, NifiIOPortApi, NifiProcessorApi}
import org.dcs.flow.{AsyncFlowUnitSpec, IT}
import org.dcs.remote.ZkRemoteService
import org.scalatest.{Assertion, Ignore}

import scala.concurrent.Future

object InputPortIngestionSpec {
  val ClientId: String = UUID.randomUUID().toString
  val FlowInstanceName = "InputPortIngestionProcessorTest"
  val ServiceClassPrefix = "org.dcs.core.service."
  val KaaIngestionProcessorService = "KaaIngestionProcessorService"

  val InputPortIngestionFlowTemplateName = "KaaTest"
  val MultipleExternalFlowTemplateName = "KaaSparkTest"

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
    // FIXME: Bad Idea to initialise service caches in one (first) test
    //        when running tests in parallel. This is workaround for putting this
    //        call at test suite initiation because it will run even if it tests
    //        are not run. Ideal solution is to tag the entire suite
    ZkRemoteService.loadServiceCaches()
    var flowInstance = flowApi.create(FlowInstanceName, ClientId).futureValue
    var processor = validateCreateInputPortIngestionProcessor(processorApi, kaaPsd, flowInstance.id)
    flowInstance = validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, FlowInstanceName, 1)
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
    flowInstance = validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, FlowInstanceName, 1)
    processor = processorApi.instance(processor.id).futureValue

    flowApi.remove(flowInstance.id, flowInstance.version, ClientId, hasExternal = true)
      .map(deleteOk => assert(deleteOk))

  }

  // FIXME: This test requires the move of update properties logic from the web project to flow,
  //        so that we can resolve the read schema dynamically

  "Instantiation / Lifecycle of Flow with an Input Ingestion Processor" should "be valid" taggedAs IT in {
    val flowTemplate = flowApi.templates().futureValue.find(_.name == InputPortIngestionFlowTemplateName).get
    val flowInstance = flowApi.instantiate(flowTemplate.id, ClientId).futureValue
    validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, InputPortIngestionFlowTemplateName, 2)
    flowApi.remove(flowInstance.id, flowInstance.version, ClientId, true).map(deleteOk => assert(deleteOk))
  }

  "Instantiation  / Lifecycle of Flow with multiple external connections" should "be valid" taggedAs IT in {
    val flowTemplate = flowApi.templates().futureValue.find(_.name == MultipleExternalFlowTemplateName).get
    val flowInstance = flowApi.instantiate(flowTemplate.id, ClientId).futureValue
    validateFlowInstanceWithInputPortIngestionProcessor(flowApi, flowInstance.id, MultipleExternalFlowTemplateName, 3)
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
                                                          flowName: String,
                                                          noOfConnections: Int): FlowInstance = {
    val flowInstance = flowApi.instance(flowInstanceId, ClientId).futureValue
    assert(flowInstance.name == flowName)
    val inputPortIngestionProcessor =
      flowInstance.processors.find(_.processorType == RemoteProcessor.InputPortIngestionType).get
    assert(flowInstance.connections.size == noOfConnections)
    val inputPortConnections = flowInstance.connections.filter(c => {
      c.config.source.componentType == FlowComponent.InputPortType &&
      c.config.destination.componentType == FlowComponent.InputPortIngestionType
    })
    assert(inputPortConnections.size == 1)
    val inputPortConnection = inputPortConnections.head

    assert(inputPortConnection.relatedConnections.size == 1)
    val rootPortConnection = inputPortConnection.relatedConnections.head
    assert(rootPortConnection.config.source.componentType == FlowComponent.InputPortType)
    assert(rootPortConnection.config.destination.componentType == FlowComponent.InputPortType)

    assert(rootPortConnection.id ==
      inputPortIngestionProcessor.properties(ExternalProcessorProperties.RootInputConnectionIdKey))
    assert(inputPortConnection.config.source.name ==
      inputPortIngestionProcessor.properties(ExternalProcessorProperties.InputPortNameKey))
    assert(rootPortConnection.config.source.id ==
      inputPortIngestionProcessor.properties(ExternalProcessorProperties.RootInputPortIdKey))
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