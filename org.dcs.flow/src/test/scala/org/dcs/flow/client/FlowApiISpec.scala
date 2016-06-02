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
  }

  "Flow Instantiation for non-existing template id" must " be invalid " taggedAs(IT) in {
    validateNonExistingFlowInstantiation(flowClient)
  }

  "Flow Retrieval " must " be valid " taggedAs(IT) in {
    validateFlowRetrieval(flowClient)

  }

  "Flow Instance Deletion " must " be valid " taggedAs(IT) in {
    validateFlowDeletion(flowClient)
  }

}
