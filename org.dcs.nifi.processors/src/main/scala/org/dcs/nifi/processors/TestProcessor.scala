package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags

@SideEffectFree
@Tags(Array("test", "hello"))
@CapabilityDescription("Generate simple hello greeting")
class TestProcessor extends RemoteProcessor {
	override def flowModuleClassName(): String = "org.dcs.core.module.flow.TestFlowModule"
}
