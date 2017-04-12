package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

/**
  * Created by cmathew on 31.03.17.
  */

@Tags(Array("worker", "stateful"))
@CapabilityDescription("Stub for a remote stateful worker processor")
@InputRequirement(Requirement.INPUT_REQUIRED)
class WorkerStatefulProcessor extends InputOutputStatefulClientProcessor
