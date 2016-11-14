package org.dcs.nifi.processors

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}


@Tags(Array("test", "hello"))
@CapabilityDescription("Retrieve GBIF Occurrences")
class StatefulGBIFOccurrenceProcessor extends OutputStatefulClientProcessor {
	override def processorClassName(): String = "org.dcs.core.service.StatefulGBIFOccurrenceProcessorService"
}
