package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}


@Tags(Array("test", "hello"))
@CapabilityDescription("Generate simple hello greeting")
class StatefulTestProcessor extends InputOutputStatefulClientProcessor {
	override def processorClassName(): String = "org.dcs.core.service.StatefulTestProcessorService"
}
