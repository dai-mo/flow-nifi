package org.dcs.flow.client

import org.dcs.flow.client.FlowApiSpec.NifiFlowApi
import org.dcs.flow.{IT, RestBaseUnitSpec}
import org.glassfish.jersey.filter.LoggingFilter


/**
  * Created by cmathew on 31/05/16.
  */
class FlowApiISpec extends RestBaseUnitSpec with FlowApiBehaviors {

  val flowClient = new NifiFlowApi()
  flowClient.requestFilter(new LoggingFilter())

  "Flow Instantiation for existing template id" must " be valid " taggedAs(IT) in {
    validateFlowInstantiation(flowClient)
    flowClient.instances(FlowApiSpec.UserId, FlowApiSpec.ClientToken).foreach(fi => {
      validateFlowRetrieval(flowClient, fi.getId())
    })
  }

  "Flow Instantiation for non-existing template id" must " be invalid " taggedAs(IT) in {
    validateNonExistingFlowInstantiation(flowClient)
  }


  "Flow Instance Deletion " must " be valid " taggedAs(IT) in {
    flowClient.instances(FlowApiSpec.UserId, FlowApiSpec.ClientToken).foreach(fi => {
      validateFlowDeletion(flowClient, fi.getId())
    })
  }

}
