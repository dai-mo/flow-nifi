package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.api.error.RESTException
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiFlowApi, NifiFlowClient, NifiProcessorClient}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by cmathew on 30/05/16.
  */
object FlowApiSpec {
  val ClientToken = "29474d0f-3e21-4136-90fd-ad4e2c613afb"
  val UserId = "root"

  val TemplateId = "d73b5a44-5968-47d5-9a9a-aea5664c5835"
  val FlowInstanceId = "d2ddd0e9-4dac-419f-bdf7-d6724a0d5daa"

  val logger: Logger = LoggerFactory.getLogger(classOf[FlowApiSpec])

}

class FlowApiSpec extends RestBaseUnitSpec with FlowApiBehaviors {
  import FlowApiSpec._

  "Templates Retrieval" must "be valid" in {

    val flowTemplatesPath: Path = Paths.get(this.getClass().getResource("templates.json").toURI())
    val flowClient = spy(new NifiFlowApi())

    doReturn(jsonFromFile(flowTemplatesPath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.TemplatesPath),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    validateTemplatesRetrieval(flowClient)
  }

  "Flow Instantiation for existing template id" must "be valid" in {

    val flowTemplatesPath: Path = Paths.get(this.getClass().getResource("templates.json").toURI())
    val templateInstancePath: Path = Paths.get(this.getClass().getResource("flow-template-instance.json").toURI())
    val createProcessGroupPath: Path = Paths.get(this.getClass().getResource("create-process-group.json").toURI())

    val flowClient = spy(new NifiFlowApi())

    doReturn(jsonFromFile(flowTemplatesPath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.TemplatesPath),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(jsonFromFile(createProcessGroupPath.toFile)).
      when(flowClient).
      postAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(UserId)),
        Matchers.any[Form],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
      )

    doReturn(jsonFromFile(templateInstancePath.toFile)).
      when(flowClient).
      postAsJson(
        Matchers.eq(NifiFlowClient.templateInstancePath(FlowInstanceId)),
        Matchers.any[Form],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
      )

    doReturn(0.0.toLong).
      when(flowClient).
      currentVersion()

    validateFlowInstantiation(flowClient, "DateConversion")
  }

  "Flow Retrieval" must "be valid" in {


    val flowInstancePath: Path = Paths.get(this.getClass().getResource("flow-instance.json").toURI())
    val flowClient = spy(new NifiFlowApi())


    doReturn(jsonFromFile(flowInstancePath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.processGroupsPath(UserId) + "/" + FlowInstanceId),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )



    doReturn(49.0.toLong).
      when(flowClient).
      currentVersion()

    validateFlowRetrieval(flowClient, FlowInstanceId)
  }

    "Flow Deletion" must "be valid" in {

      val deleteFlowPath: Path = Paths.get(this.getClass().getResource("delete-flow.json").toURI())

      val flowClient = spy(new NifiFlowApi())

      doReturn(jsonFromFile(deleteFlowPath.toFile)).
        when(flowClient).
        deleteAsJson(
          Matchers.eq(NifiFlowClient.processGroupsPath(UserId) + "/" + FlowInstanceId),
          Matchers.any[List[(String, String)]],
          Matchers.any[List[(String, String)]]
        )

      doReturn(49.0.toLong).
        when(flowClient).
        currentVersion()

      validateFlowDeletion(flowClient, FlowInstanceId)
    }
}

trait FlowApiBehaviors {
  this: FlatSpec =>

  import FlowApiSpec._


  val invalidTemplateId = "d615fb63-bc39-458c-bfcf-1f197ecdc81"
  val flowInstanceId = "3f948eeb-61d8-4f47-81f4-fff5cac50ed8"



  def validateTemplatesRetrieval(flowClient: NifiFlowClient) {
    val templates = flowClient.templates(ClientToken)
    assert(templates.size == 6)
  }

  def validateFlowInstantiation(flowClient: NifiFlowClient, name: String) {
    val flow = flowClient.instantiate(TemplateId, UserId , ClientToken)
    assert(flow.processors.size == 5)
    assert(flow.connections.size == 4)
    assert(flow.name == name)
    flow.connections.foreach(c => {
      assert(c.source.`type` == "PROCESSOR")
      assert(c.destination.`type` == "PROCESSOR")
    })

    assert(!flow.getId().isEmpty)
  }

  def validateNonExistingFlowInstantiation(flowClient: NifiFlowClient) {
    val thrown = intercept[RESTException] {
      flowClient.instantiate(invalidTemplateId, UserId, ClientToken)
    }
    assert(thrown.errorResponse.httpStatusCode == 400)
  }

  def validateFlowRetrieval(flowClient: NifiFlowClient, flowInstanceId: String) {
    val flowInstance = flowClient.instance(flowInstanceId, UserId, ClientToken)
    assert(flowInstance.processors.size == 5)
    assert(flowInstance.connections.size == 4)
  }

  def validateFlowDeletion(flowClient: NifiFlowClient, flowInstanceId: String) {
    assert(flowClient.remove(flowInstanceId, UserId, ClientToken))
  }

  def validateStartStop(flowClient: NifiFlowClient, flowInstanceId: String) {
    var processors = flowClient.start(flowInstanceId, UserId, ClientToken)
    processors.foreach(p => p.status == NifiProcessorClient.StateRunning)

    processors = flowClient.stop(flowInstanceId, UserId, ClientToken)
    processors.foreach(p => p.status == NifiProcessorClient.StateStopped)
  }
}
