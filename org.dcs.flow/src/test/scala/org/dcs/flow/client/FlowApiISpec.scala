package org.dcs.flow.client

import org.dcs.flow.{IT, RestBaseUnitSpec}
import org.dcs.flow.client.FlowApiSpec.NifiFlowClient

/**
  * Created by cmathew on 31/05/16.
  */
class FlowApiISpec extends RestBaseUnitSpec with FlowApiBehaviors {

  val flowClient = new NifiFlowClient()

  "Flow Instantiation for existing template id" must " be valid " taggedAs(IT) in {
    validateFlowInstantiation(flowClient)
  }

  "Flow Instantiation for non-existing template id" must " be invalid " taggedAs(IT) in {
    validateNonExistingFlowInstantiation(flowClient)
  }

}
