package org.dcs.nifi.processors

import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

/**
  * Created by cmathew on 31.03.17.
  */

@SideEffectFree
@Tags(Array("ingestion", "stateless"))
@CapabilityDescription("Stub for a remote stateless ingestion processor")
class IngestionProcessor extends OutputClientProcessor
