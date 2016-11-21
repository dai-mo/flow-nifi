package org.dcs.nifi.processors

/**
  * Created by cmathew on 20.11.16.
  */

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

@SideEffectFree
@Tags(Array("test", "hello"))
@CapabilityDescription("Retrieve GBIF Occurrences")
class CSVFileOutputProcessor extends InputOutputStatefulClientProcessor {
  override def processorClassName(): String = "org.dcs.core.service.CSVFileOutputProcessorService"
}

