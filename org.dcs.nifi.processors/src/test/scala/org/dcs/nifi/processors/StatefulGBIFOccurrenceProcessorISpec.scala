package org.dcs.nifi.processors

/**
  * Created by cmathew on 13.11.16.
  */

object StatefulGBIFOccurrenceProcessorISpec {
  val testProcessor: StatefulGBIFOccurrenceProcessor = new StatefulGBIFOccurrenceProcessor()

}

class StatefulGBIFOccurrenceProcessorISpec extends ProcessorsBaseUnitSpec with StatefulGBIFOccurrenceProcessorBehaviors {

  import org.dcs.nifi.processors.StatefulGBIFOccurrenceProcessorISpec._

  "Stateful GBIF Occurrence Processor Response" must " be valid " taggedAs(IT) in {

    validResponse(testProcessor)
  }
}



