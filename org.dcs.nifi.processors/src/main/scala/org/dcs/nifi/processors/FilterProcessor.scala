package org.dcs.nifi.processors

/**
  * Created by cmathew on 20.11.16.
  */

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags

@SideEffectFree
@Tags(Array("filter"))
@CapabilityDescription("Filters term values that match a given substring")
class FilterProcessor extends InputOutputClientProcessor {
  override def processorClassName(): String = "org.dcs.core.service.FilterProcessorService"
}

