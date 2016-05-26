package org.dcs.nifi.rest

import org.dcs.nifi.rest.RestApiSpec.NifiProcessorClient


  
class RestApiISpec extends RestBaseUnitSpec with RestApiBehaviors {

  "Int. Processor Types" must " be valid " taggedAs(IT) in {    
    validateProcessorTypes(NifiProcessorClient)
  }
  
}