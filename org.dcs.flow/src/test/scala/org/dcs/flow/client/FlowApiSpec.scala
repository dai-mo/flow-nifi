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

import java.nio.file.{Path, Paths}
import java.util.UUID
import javax.ws.rs.core.MediaType

import org.dcs.api.service.{FlowApiService, FlowComponent, FlowInstance, FlowTemplate}
import org.dcs.commons.error.HttpException
import org.dcs.flow.nifi._
import org.dcs.flow.{FlowUnitSpec, IT}
import org.dcs.remote.ZkRemoteService
import org.mockito.Matchers
import org.mockito.Mockito._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future


/**
  * Created by cmathew on 30/05/16.
  */
object FlowApiSpec {
  val ClientToken = "29474d0f-3e21-4136-90fd-ad4e2c613afb"
  val UserId = "root"

  val TemplateId = "a49eb70c-42e9-4736-86da-7193e1c892eb"
  val FlowInstanceId = "67a976ee-015b-1000-a69b-9a30ef4b8adc"

  val FlowName = "CleanGBIFDataWithPFields"

  val ClientId: String = UUID.randomUUID().toString

  val logger: Logger = LoggerFactory.getLogger(classOf[FlowApiSpec])

}

class FlowApiSpec extends FlowApiBehaviors {
  import FlowApiSpec._

  "Templates Retrieval" must "be valid" in {

    val flowTemplatesPath: Path = Paths.get(this.getClass.getResource("templates.json").toURI)
    val flowClient = spy(new NifiFlowApi())

    doReturn(Future.successful(jsonFromFile(flowTemplatesPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.TemplatesPath),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    validateTemplatesRetrieval(flowClient)
  }

  "Flow Instantiation for existing template id" must "be valid" in {

    val flowTemplatesPath: Path = Paths.get(this.getClass.getResource("templates.json").toURI)
    val createProcessGroupPath: Path = Paths.get(this.getClass.getResource("create-process-group.json").toURI())
    val templateInstancePath: Path = Paths.get(this.getClass.getResource("flow-template-instance.json").toURI())


    val flowClient = spy(new NifiFlowApi())

    doReturn(Future.successful(jsonFromFile(flowTemplatesPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.TemplatesPath),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(Future.successful(jsonFromFile(createProcessGroupPath.toFile)))
      .when(flowClient)
      .postAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(UserId) + "/process-groups"),
        Matchers.any[AnyRef],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_JSON)
      )

    doReturn(Future.successful(jsonFromFile(templateInstancePath.toFile))).
      when(flowClient)
      .postAsJson(
        Matchers.eq(NifiFlowClient.templateInstancePath(FlowInstanceId)),
        Matchers.any[AnyRef],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_JSON)
      )

    validateFlowInstantiation(flowClient, "CleanGBIFData", TemplateId, ClientId)
  }

  "Flow Retrieval" must "be valid" in {

    val processGroupPath: Path = Paths.get(this.getClass.getResource("process-group.json").toURI)
    val flowInstancePath: Path = Paths.get(this.getClass.getResource("flow-instance.json").toURI)
    val flowClient = spy(new NifiFlowApi())


    doReturn(Future.successful(jsonFromFile(processGroupPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(Future.successful(jsonFromFile(flowInstancePath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.flowProcessGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )


    validateFlowRetrieval(flowClient, FlowInstanceId, ClientId)
  }

//  "Flow Deletion" must "be valid" in {
//
//    val processGroupPath: Path = Paths.get(this.getClass.getResource("process-group.json").toURI)
//    val deleteFlowPath: Path = Paths.get(this.getClass.getResource("delete-flow.json").toURI)
//
//    val flowClient = spy(new NifiFlowApi())
//
//    doReturn(Future.successful(jsonFromFile(processGroupPath.toFile)))
//      .when(flowClient)
//      .getAsJson(
//        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
//        Matchers.any[List[(String, String)]],
//        Matchers.any[List[(String, String)]]
//      )
//
//
//    doReturn(Future.successful(jsonFromFile(deleteFlowPath.toFile)))
//      .when(flowClient)
//      .deleteAsJson(
//        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
//        Matchers.any[List[(String, String)]],
//        Matchers.any[List[(String, String)]]
//      )
//
//    validateFlowDeletion(flowClient, FlowInstanceId, 1, ClientId)
//  }
}

class FlowApiISpec extends FlowApiBehaviors
  with ProcessorApiBehaviors
  with ProvenanceApiBehaviours {
  import FlowApiSpec._

  val flowClient = new NifiFlowApi

  val processorClient = new NifiProcessorApi

  val provenanceClient = new NifiProvenanceApi

  val ClientId: String = UUID.randomUUID().toString



  "Flow Instantiation" must "be valid  for existing template id" taggedAs IT in {
    // FIXME: Bad Idea to initialise service caches in one (first) test
    //        when running tests in parallel. This is workaround for putting this
    //        call at test suite initiation because it will run even if it tests
    //        are not run. Ideal solution is to tag the entire suite
    ZkRemoteService.loadServiceCaches()
    val templateId = flowClient.templates().futureValue.find(t => t.name == FlowName).get.getId
    var fi = validateFlowInstantiation(flowClient, FlowName, templateId, ClientId)
    fi = validateFlowRetrieval(flowClient, fi.getId, ClientId)
    validateFlowInstance(fi)
    validateFlowDeletion(flowClient, fi.getId, fi.version, ClientId)
  }

  "Flow Instantiation" must "be invalid for non-existing template id" taggedAs IT in {
    validateNonExistingFlowInstantiation(flowClient, ClientId)
  }

  "Flow Instance State Change" must "result in valid state" taggedAs IT in {
    val templateId = flowClient.templates().futureValue.find(t => t.name == FlowName).get.getId
    validateRun(flowClient, templateId)
  }


}

trait FlowApiBehaviors extends FlowUnitSpec {
  import FlowApiSpec._

  val invalidTemplateId = "invalid-template-id"
  val flowInstanceId = "3f948eeb-61d8-4f47-81f4-fff5cac50ed8"

  def validateTemplatesRetrieval(flowClient: NifiFlowClient): List[FlowTemplate] = {
    val templates = flowClient.templates().futureValue
    assert (templates.size == 2)
    templates
  }

  def validateFlowInstantiation(flowClient: NifiFlowClient, name: String, templateId: String, clientId: String): FlowInstance = {
    val flow = flowClient.instantiate(templateId, clientId).futureValue
    assert(flow.processors.size == 4)
    assert(flow.connections.size == 3)
    assert(flow.name == name)
    flow.connections.foreach(c => {
      assert(c.config.source.componentType == FlowComponent.ProcessorType)
      assert(c.config.destination.componentType == FlowComponent.ProcessorType)
    })
    assert(!flow.getId.isEmpty)
    flow
  }

  def validateNonExistingFlowInstantiation(flowClient: NifiFlowClient, clientId: String) {
    whenReady(flowClient.instantiate(invalidTemplateId, clientId).failed) { ex =>
      ex shouldBe an [HttpException]
      assert(ex.asInstanceOf[HttpException].errorResponse.httpStatusCode == 400)
    }
  }

  def validateFlowRetrieval(flowClient: NifiFlowClient, flowInstanceId: String, clientId: String): FlowInstance = {
    val flowInstance = flowClient.instance(flowInstanceId, clientId).futureValue
    assert(flowInstance.processors.size == 4)
    assert(flowInstance.connections.size == 3)
    flowInstance
  }

  def validateFlowInstance(flowInstance: FlowInstance) {
    assert(flowInstance.processors.size == 4)
    assert(flowInstance.connections.size == 3)
  }

  def validateFlowDeletion(flowClient: FlowApiService, flowInstanceId: String, version: Long, clientId: String, hasExternal: Boolean = false) {
    assert(flowClient.remove(flowInstanceId, version, clientId, hasExternal).futureValue)
  }

  def validateStart(flowClient: FlowApiService, flowInstanceId: String, clientId: String): FlowInstance = {
    val flowInstance = flowClient.start(flowInstanceId, clientId).futureValue
    assert(flowInstance.state == NifiProcessorClient.StateRunning)
    val processors = flowClient.instance(flowInstanceId, clientId).futureValue.processors
    processors.foreach(p => p.status == NifiProcessorClient.StateRunning)
    flowInstance
  }

  def validateStop(flowClient: FlowApiService, flowInstanceId: String, clientId: String): FlowInstance = {
    val flowInstance = flowClient.stop(flowInstanceId, clientId).futureValue
    assert(flowInstance.state == NifiProcessorClient.StateStopped)
    val processors = flowClient.instance(flowInstanceId, clientId).futureValue.processors
    processors.foreach(p => p.status == NifiProcessorClient.StateStopped)
    flowInstance
  }

  def validateRun(flowClient: FlowApiService,
                  templateId: String,
                  hasExternal: Boolean = false): Unit = {
    // Instantiate a flow instance from an existing flow template
    var flowInstance = flowClient.instantiate(templateId, ClientId).futureValue
    // Start the flow
    flowInstance = validateStart(flowClient, flowInstance.id, ClientId)
    // Wait a bit to allow processors to generate output
    Thread.sleep(10000)
    // Stop the flow
    flowInstance = validateStop(flowClient, flowInstance.id, ClientId)
    Thread.sleep(5000)
    // Delete the flow
    validateFlowDeletion(flowClient, flowInstance.getId, flowInstance.version, ClientId, hasExternal)
  }
}
