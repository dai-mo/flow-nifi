package org.dcs.nifi.repository

import org.dcs.nifi.FlowDataProvenance

/**
  * Created by cmathew on 16.01.17.
  */
class DCSProvenanceRepository extends BaseProvenanceRepository {

  override def getMaxEventId: java.lang.Long = {
    import ctx._

    val latestEventIdQuery = quote(query[FlowDataProvenance].map(fdp => fdp.eventId))
    val latestEventId = ctx.run(latestEventIdQuery.max)
    if (latestEventId.isEmpty)
      null
    else
      latestEventId.get.toLong
  }
}

