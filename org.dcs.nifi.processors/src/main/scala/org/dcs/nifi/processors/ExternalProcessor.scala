package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.processor.{ProcessContext, ProcessSession}

@Tags(Array("external", "stateful"))
@CapabilityDescription("Stub for a remote external processor")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
class ExternalProcessor extends NoInputOutputStatefulClientProcessor {

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    context.`yield`()
  }
}
