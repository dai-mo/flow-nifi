package org.dcs.nifi.repository

import java.util

import io.getquill.{CassandraSyncContext, SnakeCase}
import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.provenance.ProvenanceEventRecord
import org.dcs.nifi.FlowDataProvenance

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 16.01.17.
  */

class QuillContext extends CassandraSyncContext[SnakeCase]("cassandra")

class CassandraProvenanceRepository extends SQLProvenanceRepository {

  override def getEvents(firstRecordId: Long, maxRecords: Int, user: NiFiUser): util.List[ProvenanceEventRecord] = {
    import ctx._

    val provQuery = quote(query[FlowDataProvenance].filter(fdp =>
      fdp.eventId >= lift(firstRecordId.toDouble) && fdp.eventId < lift(firstRecordId.toDouble + maxRecords)
    ).allowFiltering)
    ctx.run(provQuery).map(_.toProvenanceEventRecord()).sortBy(_.getEventId).asJava
  }
}
