package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import java.util.UUID
import javax.ws.rs.core.MediaType

import org.dcs.api.service.{FlowComponent, FlowInstance, FlowTemplate}
import org.dcs.commons.error.HttpException
import org.dcs.flow.nifi.{NifiFlowApi, NifiFlowClient, NifiProcessorClient}
import org.dcs.flow.{FlowBaseUnitSpec, FlowUnitSpec}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec
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

  val ClientId: String = UUID.randomUUID().toString

  val logger: Logger = LoggerFactory.getLogger(classOf[FlowApiSpec])

}

class FlowApiSpec extends FlowUnitSpec with FlowApiBehaviors {
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


    validateFlowRetrieval(flowClient, FlowInstanceId)
  }

  "Flow Deletion" must "be valid" in {

    val processGroupPath: Path = Paths.get(this.getClass.getResource("process-group.json").toURI)
    val deleteFlowPath: Path = Paths.get(this.getClass.getResource("delete-flow.json").toURI)

    val flowClient = spy(new NifiFlowApi())

    doReturn(Future.successful(jsonFromFile(processGroupPath.toFile)))
      .when(flowClient)
      .getAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )


    doReturn(Future.successful(jsonFromFile(deleteFlowPath.toFile)))
      .when(flowClient)
      .deleteAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(FlowInstanceId)),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    validateFlowDeletion(flowClient, FlowInstanceId, 1, ClientId)
  }
}

trait FlowApiBehaviors extends FlowBaseUnitSpec {
  this: FlatSpec =>

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

  def validateFlowRetrieval(flowClient: NifiFlowClient, flowInstanceId: String): FlowInstance = {
    val flowInstance = flowClient.instance(flowInstanceId).futureValue
    assert(flowInstance.processors.size == 4)
    assert(flowInstance.connections.size == 3)
    flowInstance
  }

  def validateFlowInstance(flowInstance: FlowInstance) {
    assert(flowInstance.processors.size == 4)
    assert(flowInstance.connections.size == 3)
  }

  def validateFlowDeletion(flowClient: NifiFlowClient, flowInstanceId: String, version: Long, clientId: String) {
    assert(flowClient.remove(flowInstanceId, version, clientId).futureValue)
  }

  def validateStart(flowClient: NifiFlowClient, flowInstanceId: String): FlowInstance = {
    val flowInstance = flowClient.start(flowInstanceId).futureValue
    assert(flowInstance.state == NifiProcessorClient.StateRunning)
    val processors = flowClient.instance(flowInstanceId).futureValue.processors
    processors.foreach(p => p.status == NifiProcessorClient.StateRunning)
    flowInstance
  }

  def validateStop(flowClient: NifiFlowClient, flowInstanceId: String): FlowInstance = {
    val flowInstance = flowClient.stop(flowInstanceId).futureValue
    assert(flowInstance.state == NifiProcessorClient.StateStopped)
    val processors = flowClient.instance(flowInstanceId).futureValue.processors
    processors.foreach(p => p.status == NifiProcessorClient.StateStopped)
    flowInstance
  }
}
