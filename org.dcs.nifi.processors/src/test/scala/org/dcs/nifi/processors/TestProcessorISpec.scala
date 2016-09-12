package org.dcs.nifi.processors


import org.dcs.nifi.processors.TestProcessorISpec._



object TestProcessorISpec {
  val testProcessor: TestProcessor = new TestProcessor()
  
}


class TestProcessorISpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {
  "Test Processor Response" must " be valid " taggedAs(IT) in {
    validResponse(testProcessor)
  }
}