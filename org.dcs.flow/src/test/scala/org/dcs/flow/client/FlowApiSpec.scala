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


  def validateFlowInstantiation(flowClient: NifiFlowClient) {
    val flow = flowClient.instantiate(templateId, ClientToken)
    assert(flow.processors.size == 5)
    assert(flow.connections.size == 4)
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