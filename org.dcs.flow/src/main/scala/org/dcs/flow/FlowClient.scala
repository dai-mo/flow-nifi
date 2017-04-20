package org.dcs.flow

import org.dcs.flow.nifi.{NifiConnectionApi, NifiFlowApi, NifiProcessorApi, NifiProvenanceApi}

/**
  * Created by cmathew on 05/08/16.
  */
object ProcessorApi extends NifiProcessorApi

object FlowApi extends NifiFlowApi

object ProvenanceApi extends NifiProvenanceApi

object ConnectionApi extends NifiConnectionApi