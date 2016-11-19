package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

@SideEffectFree
@Tags(Array("latitude", "longitude", "validation"))
@CapabilityDescription("Validate latitude / longitude values")
class LatLongValidationProcessor extends InputOutputClientProcessor {
	override def processorClassName(): String = "org.dcs.core.service.LatLongValidationProcessorService"
}
