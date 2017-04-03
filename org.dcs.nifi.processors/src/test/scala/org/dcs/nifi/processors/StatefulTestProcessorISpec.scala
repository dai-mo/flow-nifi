package org.dcs.nifi.processors


class StatefulTestProcessorISpec extends ProcessorsBaseUnitSpec with StatefulTestProcessorBehaviors {

  import StatefulTestProcessorSpec._

  "Stateful Test Processor Response" must " be valid " taggedAs(IT) in {
    val testProcessor1: WorkerStatefulProcessor = new WorkerStatefulProcessor ()
    testProcessor1.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)

    val testProcessor2: WorkerStatefulProcessor  = new WorkerStatefulProcessor ()
    testProcessor2.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)

    assert(validResponse(testProcessor1) != validResponse(testProcessor2))
    assert(testProcessor1.processorStateId != testProcessor2.processorStateId)
  }

  override def processorServiceClassName: String = ProcessorServiceClassName
}


