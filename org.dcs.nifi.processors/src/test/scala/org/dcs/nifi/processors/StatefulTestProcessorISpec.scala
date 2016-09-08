package org.dcs.nifi.processors


object StatefulTestProcessorISpec {
  val testProcessor1: StatefulTestProcessor = new StatefulTestProcessor()
  val testProcessor2: StatefulTestProcessor = new StatefulTestProcessor()
}

class StatefulTestProcessorISpec extends ProcessorsBaseUnitSpec with StatefulTestProcessorBehaviors {

  import org.dcs.nifi.processors.StatefulTestProcessorISpec._

  "Stateful Test Processor Response" must " be valid " taggedAs(IT) in {

    assert(validResponse(testProcessor1) != validResponse(testProcessor2))
    assert(testProcessor1.processorStateId != testProcessor2.processorStateId)
  }
}


