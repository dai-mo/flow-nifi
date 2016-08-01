package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.api.error.RESTException
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiApiConfig, NifiFlowClient}
import org.mockito.Matchers
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by cmathew on 30/05/16.
  */
object FlowApiSpec {
  val ClientToken = "29474d0f-3e21-4136-90fd-ad4e2c613afb"
  class NifiFlowApi extends NifiFlowClient
    with NifiApiConfig
}

class FlowApiSpec extends RestBaseUnitSpec with FlowApiBehaviors {
  import FlowApiSpec._

  "Templates Retrieval " must " be valid " in {

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

  "Flow Instantiation for existing template id" must " be valid " in {

    val templatePath: Path = Paths.get(this.getClass().getResource("flow-template.json").toURI())
    val flowClient = spy(new NifiFlowApi())

    doReturn(jsonFromFile(templatePath.toFile)).
      when(flowClient).
      postAsJson(
        Matchers.eq(NifiFlowClient.TemplateInstancePath),
        Matchers.any[Form],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
      )

    doReturn(0.0.toLong).
      when(flowClient).
      currentVersion()

    validateFlowInstantiation(flowClient)
  }

  "Flow Retrieval " must " be valid " in {

    val flowInstancePath: Path = Paths.get(this.getClass().getResource("flow-instance.json").toURI())
    val flowClient = spy(new NifiFlowApi())


    doReturn(jsonFromFile(flowInstancePath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.ProcessGroupsPath + "/root"),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )



    doReturn(49.0.toLong).
      when(flowClient).
      currentVersion()

    validateFlowRetrieval(flowClient)
  }

  "Flow Deletion " must " be valid " in {

    val flowInstancePath: Path = Paths.get(this.getClass().getResource("flow-instance.json").toURI())
    val registerSnippetPath: Path = Paths.get(this.getClass().getResource("flow-snippet.json").toURI())
    val deleteSnippetPath: Path = Paths.get(this.getClass().getResource("delete-flow.json").toURI())

    val flowClient = spy(new NifiFlowApi())


    doReturn(jsonFromFile(flowInstancePath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.ProcessGroupsPath + "/root"),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(jsonFromFile(registerSnippetPath.toFile)).
      when(flowClient).
      postAsJson(
        Matchers.eq(NifiFlowClient.SnippetsPath),
        Matchers.any[Form],
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]],
        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
      )

    doReturn(jsonFromFile(flowInstancePath.toFile)).
      when(flowClient).
      getAsJson(
        Matchers.eq(NifiFlowClient.SnippetsPath + "/c484a82c-27a0-4ceb-b9a2-916e8b8000f6"),
        Matchers.any[List[(String, String)]],
        Matchers.any[List[(String, String)]]
      )

    doReturn(49.0.toLong).
      when(flowClient).
      currentVersion()

    validateFlowRetrieval(flowClient)
  }
}

trait FlowApiBehaviors { this: FlatSpec =>
  import FlowApiSpec._

  val templateId = "d73b5a44-5968-47d5-9a9a-aea5664c5835"
  val invalidTemplateId = "d615fb63-bc39-458c-bfcf-1f197ecdc81"

  val logger: Logger = LoggerFactory.getLogger(classOf[FlowApiSpec])

  def validateTemplatesRetrieval(flowClient: NifiFlowClient) {
    val templates = flowClient.templates(ClientToken)
    assert(templates.size == 6)
  }

  def validateFlowInstantiation(flowClient: NifiFlowClient) {
    val flow = flowClient.instantiate(templateId, ClientToken)
    assert(flow.processors.size == 5)
    assert(flow.connections.size == 4)

    val actualSourcePortIds = Set("30627450-069c-4474-abdd-0a9ec7996b2a",
      "9271fe72-86db-4966-b63d-598d82c39ca7",
      "8de57c1c-4bb5-4231-b205-df27cdfab7af",
      "ce991a08-d775-4f8e-b4e6-d687a143fe98")

    val actualDestinationPortIds = Set("aee1eac7-f1b3-45b5-b4c4-4ec5a850e8ea",
      "ce991a08-d775-4f8e-b4e6-d687a143fe98",
      "9271fe72-86db-4966-b63d-598d82c39ca7",
      "30627450-069c-4474-abdd-0a9ec7996b2a")

    flow.connections.foreach(c => {
      assert(actualSourcePortIds.contains(c.source.id))
      assert(c.source.`type` == "PROCESSOR")
      assert(actualDestinationPortIds.contains(c.destination.id))
      assert(c.destination.`type` == "PROCESSOR")
    })
  }

  def validateNonExistingFlowInstantiation(flowClient: NifiFlowClient) {
    val thrown = intercept[RESTException] {
      flowClient.instantiate(invalidTemplateId, ClientToken)
    }
    assert(thrown.errorResponse.httpStatusCode == 404)
  }

  def validateFlowRetrieval(flowClient: NifiFlowClient) {
    val flowInstance = flowClient.instance("root", ClientToken)
    assert(flowInstance.processors.size == 5)
    assert(flowInstance.connections.size == 4)
  }

  def validateFlowDeletion(flowClient: NifiFlowClient) {
    assert(flowClient.remove("root", ClientToken))

  }
}
