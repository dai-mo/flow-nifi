package org.dcs.nifi.repository

import java.util

import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.provenance.ProvenanceEventRecord
import org.dcs.nifi.{FlowDataProvenance, FlowId, FlowProvenanceEventRecord}

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 16.01.17.
  */


class DCSProvenanceRepository extends BaseProvenanceRepository {

  private val eventIdIncrementor = new EventIdIncrementor
  private val ProvenanceEventName = "provenance_event_id"

  override def getEvents(firstRecordId: Long, maxRecords: Int, user: NiFiUser): util.List[ProvenanceEventRecord] = {
    import ctx._


    val provQuery = quote(query[FlowDataProvenance].filter(fdp =>
      fdp.eventId >= lift(firstRecordId.toDouble) && fdp.eventId < lift(firstRecordId.toDouble + maxRecords)
    ).allowFiltering)
    ctx.run(provQuery).map(_.toProvenanceEventRecord()).sortBy(_.getEventId).asJava
  }

  override def registerEvent(event: ProvenanceEventRecord): Unit = {
    import ctx._
    val nextEventId = eventIdIncrementor.nextEventId()
    val fdp: FlowDataProvenance = FlowProvenanceEventRecord.toFlowDataProvenance(event, Some(nextEventId))

    val eventInsert = quote(query[FlowDataProvenance].insert(lift(fdp)))
    ctx.run(eventInsert)
  }



  class EventIdIncrementor {
    // FIXME: Not efficient, but since Nifi uses incremental ids this is required
    def nextEventId(): Double = synchronized {
      import ctx._

      val nextEventIdUpdate = quote(query[FlowId].filter(_.name == lift(ProvenanceEventName)).update(fdp => fdp.latestId -> (fdp.latestId + 1)))
      ctx.run(nextEventIdUpdate)
      val latestEventIdQuery = quote(query[FlowId].filter(fid => fid.name == lift(ProvenanceEventName)))
      val latestEventId = ctx.run(latestEventIdQuery).head.latestId
      latestEventId
    }
  }

  override def getMaxEventId: java.lang.Long = {
    import ctx._

    val latestEventIdQuery = quote(query[FlowId].filter(fid => fid.name == lift(ProvenanceEventName)))
    val latestEventId = ctx.run(latestEventIdQuery)
    if(latestEventId.isEmpty)
      null
    else
      latestEventId.head.latestId.toLong
  }

  override def purge(): Unit = {
    import ctx._

    val provenancePurge = quote {
      query[FlowDataProvenance]
        .delete
    }
    ctx.run(provenancePurge)

    val flowIdPurge = quote {
      query[FlowId]
        .delete
    }
    ctx.run(flowIdPurge)
  }
}
