package org.dcs.flow.client


import java.util.UUID

import org.dcs.api.service.ProcessorInstance
import org.dcs.flow.nifi.{NifiFlowApi, NifiProcessorApi, NifiProvenanceApi}
import org.dcs.flow.{DetailedLoggingFilter, FlowUnitSpec, IT}
import org.glassfish.jersey.filter.LoggingFilter


/**
  * Created by cmathew on 31/05/16.
  */
class FlowApiISpec extends FlowUnitSpec
  with FlowApiBehaviors
  with ProcessorApiBehaviors
  with ProvenanceApiBehaviours {

  val flowClient = new NifiFlowApi
  flowClient.requestFilter(new LoggingFilter)
  flowClient.requestFilter(new DetailedLoggingFilter)

  val processorClient = new NifiProcessorApi

  val provenanceClient = new NifiProvenanceApi

  val ClientId: String = UUID.randomUUID().toString

  "Flow Instantiation" must "be valid  for existing template id" taggedAs IT in {
    val templateId = flowClient.templates().futureValue.find(t => t.name == "CleanGBIFData").get.getId
    var fi = validateFlowInstantiation(flowClient, "CleanGBIFData", templateId, ClientId)
    fi = validateFlowRetrieval(flowClient, fi.getId)
    validateFlowInstance(fi)
    validateFlowDeletion(flowClient, fi.getId, fi.version, ClientId)
  }

  "Flow Instantiation" must "be invalid for non-existing template id" taggedAs IT in {
    validateNonExistingFlowInstantiation(flowClient, ClientId)
  }

  "Flow Instance State Change" must "result in valid state" taggedAs IT in {
    val templateId = flowClient.templates().futureValue.find(t => t.name == "CleanGBIFData").get.getId
    // Instantiate a flow instance from an existing flow template
    var flowInstance = flowClient.instantiate(templateId, ClientId).futureValue
    // Start the flow
    flowInstance = validateStart(flowClient, flowInstance.id)
    // Wait a bit to allow processors to generate output
    Thread.sleep(10000)
    // Stop the flow
    flowInstance = validateStop(flowClient, flowInstance.id)
    // Delete the flow
    validateFlowDeletion(flowClient, flowInstance.getId, flowInstance.version, ClientId)
  }


}
