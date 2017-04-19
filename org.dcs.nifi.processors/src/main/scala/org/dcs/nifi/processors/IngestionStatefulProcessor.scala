package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

/**
  * Created by cmathew on 31.03.17.
  */

@Tags(Array("ingestion", "stateful"))
@CapabilityDescription("Stub for a remote stateful ingestion processor")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
class IngestionStatefulProcessor extends OutputStatefulClientProcessor
