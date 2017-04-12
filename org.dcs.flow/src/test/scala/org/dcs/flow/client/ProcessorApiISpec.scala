package org.dcs.flow.client

import org.dcs.flow.{FlowBaseUnitSpec, FlowUnitSpec, IT}
import org.dcs.flow.nifi.NifiProcessorApi
import org.scalatest.Ignore



@Ignore // FIXME: Update tests to latest nifi api
class ProcessorApiISpec extends FlowUnitSpec with ProcessorApiBehaviors {


  "Int. Processor Types" must " be valid " taggedAs(IT) in {
    validateProcessorTypes(new NifiProcessorApi())
  }

  "Int. Processor Lifecycle" must " be valid " taggedAs(IT) in {
    validateProcessorLifecycle(new NifiProcessorApi())
  }
  
}