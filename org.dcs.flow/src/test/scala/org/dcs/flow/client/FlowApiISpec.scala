package org.dcs.flow.client


import org.dcs.api.error.RESTException
import org.dcs.api.service.ProcessorInstance
import org.dcs.flow.nifi.{NifiFlowApi, NifiProcessorApi, NifiProvenanceApi, Provenance}
import org.dcs.flow.{IT, RestBaseUnitSpec}
import org.glassfish.jersey.filter.LoggingFilter


/**
  * Created by cmathew on 31/05/16.
  */
class FlowApiISpec extends RestBaseUnitSpec
  with FlowApiBehaviors
  with ProcessorApiBehaviors
  with ProvenanceApiBehaviours {

  val flowClient = new NifiFlowApi
  flowClient.requestFilter(new LoggingFilter())

  val processorClient = new NifiProcessorApi
  processorClient.requestFilter(new LoggingFilter())

  val provenanceClient = new NifiProvenanceApi
  provenanceClient.requestFilter(new LoggingFilter())


  "Flow Instantiation" must "be valid  for existing template id" taggedAs IT in {
    validateFlowInstantiation(flowClient, "DateConversion", FlowApiSpec.TemplateId)
    flowClient.instances(FlowApiSpec.UserId, FlowApiSpec.ClientToken).foreach(fi => {
      validateFlowRetrieval(flowClient, fi.getId)
      validateFlowInstance(fi)
    })
  }

//  "Flow Instantiation" must "be invalid for non-existing template id" taggedAs IT in {
//    validateNonExistingFlowInstantiation(flowClient)
//  }
//
//  "Flow Instance State Change" must "result in valid state" taggedAs IT in {
//    // Instantiate a flow instance from an existing flow template
//    val flowInstance = flowClient.instantiate(FlowApiSpec.TemplateId, FlowApiSpec.UserId, FlowApiSpec.ClientToken)
//    // Start the flow i.e. start all the processors of the flow
//    val processors: List[ProcessorInstance] = validateStart(flowClient, flowInstance.id)
//    // Wait a bit to allow processors to generate output
//    Thread.sleep(50000)
//    // Check that provenance data has been written
//    processors.foreach(p => {
//      val results = validateProvenanceRetrieval(provenanceClient,p.id)
//      Thread.sleep(5000)
//      results.foreach( r => {
//        // Check that all provenance queries have been deleted
//        val thrown = intercept[RESTException] {
//          provenanceClient.provenanceQuery(r.queryId)
//        }
//        assert(thrown.errorResponse.httpStatusCode == 500)
//      })
//    })
//    // Stop the flow i.e. stop all the processors of the flow
//    validateStop(flowClient, flowInstance.id)
//  }
//
//
//
//  "Flow Instance Deletion" must "be valid" taggedAs IT in {
//    flowClient.instances(FlowApiSpec.UserId, FlowApiSpec.ClientToken).foreach(fi => {
//      validateFlowDeletion(flowClient, fi.getId)
//    })
//  }

}
