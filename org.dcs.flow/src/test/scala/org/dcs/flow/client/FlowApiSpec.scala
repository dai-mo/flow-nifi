package org.dcs.flow.client

import java.nio.file.{Path, Paths}
import javax.ws.rs.core.{Form, MediaType}

import org.dcs.api.service.RESTException
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
  class NifiFlowApi extends FlowApi
    with NifiFlowClient
    with NifiApiConfig
}

class FlowApiSpec extends RestBaseUnitSpec with FlowApiBehaviors {
  import FlowApiSpec._


  "Flow Instantiation for existing template id" must " be valid " in {

    val templatePath: Path = Paths.get(this.getClass().getResource("dateconv-template.json").toURI())
    val flowClient = spy(new NifiFlowApi())

    doReturn(jsonFromFile(templatePath.toFile)).
      when(flowClient).
      postAsJson(
        Matchers.eq(NifiFlowClient.TemplateInstancePath),
        Matchers.any[Form],
        Matchers.any[Map[String, String]],
        Matchers.any[Map[String, String]],
        Matchers.eq(MediaType.APPLICATION_FORM_URLENCODED)
      )

    doReturn(0.0.toLong).
      when(flowClient).
      currentVersion()

    validateFlowInstantiation(flowClient)
  }


}

trait FlowApiBehaviors { this: FlatSpec =>

  val templateId = "d615fb63-bc39-458c-bfcf-1f197ecdc817"
  val invalidTemplateId = "d615fb63-bc39-458c-bfcf-1f197ecdc81"

  val logger: Logger = LoggerFactory.getLogger(classOf[FlowApiSpec])

  def validateTemplates(flowClient: NifiFlowClient): Unit = {

  }

  def validateFlowInstantiation(flowClient: NifiFlowClient) {
    val flow = flowClient.instantiate(templateId)
    assert(flow.processors.size == 5)
    assert(flow.connections.size == 4)
  }

  def validateNonExistingFlowInstantiation(flowClient: NifiFlowClient) {
    val thrown = intercept[RESTException] {
      flowClient.instantiate(invalidTemplateId)
    }
    assert(thrown.getErrorResponse.getHttpStatusCode == 404)
  }
}