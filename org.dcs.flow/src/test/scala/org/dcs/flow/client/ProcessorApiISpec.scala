package org.dcs.flow.client

import org.dcs.flow.IT
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.client.ProcessorApiSpec.NifiProcessorClient

  
class ProcessorApiISpec extends RestBaseUnitSpec with ProcessorApiBehaviors {

  "Int. Processor Types" must " be valid " taggedAs(IT) in {    
    validateProcessorTypes(new NifiProcessorClient())
  }
  
}