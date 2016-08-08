package org.dcs.flow.client


import org.dcs.flow.nifi.{NifiFlowApi, NifiProcessorApi}
import org.dcs.flow.{IT, RestBaseUnitSpec}
import org.glassfish.jersey.filter.LoggingFilter


/**
  * Created by cmathew on 31/05/16.
  */
class FlowApiISpec extends RestBaseUnitSpec with FlowApiBehaviors with ProcessorApiBehaviors{

  val flowClient = new NifiFlowApi()
  flowClient.requestFilter(new LoggingFilter())

  val processorClient = new NifiProcessorApi()
  processorClient.requestFilter(new LoggingFilter())

  "Flow Instantiation" must "be valid  for existing template id" taggedAs(IT) in {
    validateFlowInstantiation(flowClient, "DateConversion")
    flowClient.instances(FlowApiSpec.UserId, FlowApiSpec.ClientToken).foreach(fi => {
      validateFlowRetrieval(flowClient, fi.getId())
    })
  }

//  "Flow Instantiation" must "be invalid for non-existing template id" taggedAs(IT) in {
//    validateNonExistingFlowInstantiation(flowClient)
//  }
//
//  "Flow Instance State Change" must "result in valid state" taggedAs(IT) in {
//    val flowInstance = flowClient.instantiate(FlowApiSpec.TemplateId, FlowApiSpec.UserId, FlowApiSpec.ClientToken)
//    validateStartStop(flowClient, flowInstance.id)
//
//  }


//  "Flow Instance Deletion" must "be valid" taggedAs IT in {
//    flowClient.instances(FlowApiSpec.UserId, FlowApiSpec.ClientToken).foreach(fi => {
//      validateFlowDeletion(flowClient, fi.getId)
//    })
//  }

}
