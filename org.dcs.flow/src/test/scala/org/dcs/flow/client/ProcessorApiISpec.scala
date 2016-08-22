package org.dcs.flow.client

import org.dcs.flow.IT
import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.NifiProcessorApi
import org.scalatest.Ignore



@Ignore // FIXME: Update tests to latest nifi api
class ProcessorApiISpec extends RestBaseUnitSpec with ProcessorApiBehaviors {


  "Int. Processor Types" must " be valid " taggedAs(IT) in {
    validateProcessorTypes(new NifiProcessorApi())
  }

  "Int. Processor Lifecycle" must " be valid " taggedAs(IT) in {
    validateProcessorLifecycle(new NifiProcessorApi())
  }
  
}