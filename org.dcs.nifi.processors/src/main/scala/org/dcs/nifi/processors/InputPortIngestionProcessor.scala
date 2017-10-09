package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

@Tags(Array("port-ingestion", "stateless"))
@CapabilityDescription("Stub for a remote ingestion processor connected to an input port")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
class InputPortIngestionProcessor extends InputOutputClientProcessor
