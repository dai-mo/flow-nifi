package org.dcs.nifi.processors

/**
  * Created by cmathew on 13.11.16.
  */

class StatefulGBIFOccurrenceProcessorISpec extends ProcessorsBaseUnitSpec with StatefulGBIFOccurrenceProcessorBehaviors {

  import StatefulGBIFOccurrenceProcessorSpec._

  "Stateful GBIF Occurrence Processor Response" must " be valid " taggedAs(IT) in {
    val gbifOccurrenceProcessor: IngestionStatefulProcessor = new IngestionStatefulProcessor()
    gbifOccurrenceProcessor.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)
    validResponse(gbifOccurrenceProcessor)
  }

  override def processorServiceClassName: String = ProcessorServiceClassName
}



