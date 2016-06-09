package org.dcs.flow.client

import org.dcs.flow.IT
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.client.ProcessorApiSpec.NifiProcessorApi


class ProcessorApiISpec extends RestBaseUnitSpec with ProcessorApiBehaviors {

  "Int. Processor Types" must " be valid " taggedAs(IT) in {    
    validateProcessorTypes(new NifiProcessorApi())
  }

  "Int. Processor Lifecycle" must " be valid " taggedAs(IT) in {
    validateProcessorLifecycle(new NifiProcessorApi())
  }
  
}