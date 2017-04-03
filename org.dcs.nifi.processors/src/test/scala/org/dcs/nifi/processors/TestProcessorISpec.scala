package org.dcs.nifi.processors

import org.dcs.commons.serde.AvroSchemaStore

class TestProcessorISpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {
  import TestProcessorSpec._

  "Test Processor Response" must " be valid " taggedAs(IT) in {
    AvroSchemaStore.add(TestResponseSchemaId)
    val testProcessor: WorkerProcessor = new WorkerProcessor()
    testProcessor.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)
    validResponse(testProcessor)
  }

  override def processorServiceClassName: String = ProcessorServiceClassName
}