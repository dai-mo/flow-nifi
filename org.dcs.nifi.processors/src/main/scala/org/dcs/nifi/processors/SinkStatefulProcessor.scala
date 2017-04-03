package org.dcs.nifi.processors

import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}

/**
  * Created by cmathew on 31.03.17.
  */

@Tags(Array("sink", "stateful"))
@CapabilityDescription("Stub for a remote stateful sink processor")
class SinkStatefulProcessor extends InputOutputStatefulClientProcessor
