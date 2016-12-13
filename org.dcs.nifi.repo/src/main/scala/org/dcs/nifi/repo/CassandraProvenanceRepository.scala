package org.dcs.nifi.repo

import java.lang.{Iterable, Long}
import java.util

import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.events.EventReporter
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission
import org.apache.nifi.provenance.search.{Query, QuerySubmission, SearchableField}
import org.apache.nifi.provenance._

/**
  * Created by cmathew on 13.12.16.
  */
class CassandraProvenanceRepository extends ProvenanceRepository {

  override def getSearchableFields: util.List[SearchableField] = new ArrayList

  override def retrieveLineageSubmission(lineageIdentifier: String, user: NiFiUser): ComputeLineageSubmission = ???

  override def retrieveQuerySubmission(queryIdentifier: String, user: NiFiUser): QuerySubmission = ???

  override def submitLineageComputation(flowFileUuid: String, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def submitLineageComputation(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def submitExpandParents(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def getProvenanceEventRepository: ProvenanceEventRepository = this

  override def initialize(eventReporter: EventReporter, authorizer: Authorizer, resourceFactory: ProvenanceAuthorizableFactory): Unit = ???

  override def getEvents(firstRecordId: Long, maxRecords: Int, user: NiFiUser): util.List[ProvenanceEventRecord] = ???

  override def submitExpandChildren(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def getEvent(id: Long, user: NiFiUser): ProvenanceEventRecord = ???

  override def submitQuery(query: Query, user: NiFiUser): QuerySubmission = ???

  override def getSearchableAttributes: util.List[SearchableField] = ???

  override def registerEvent(event: ProvenanceEventRecord): Unit = ???

  override def registerEvents(events: Iterable[ProvenanceEventRecord]): Unit = ???

  override def getMaxEventId: Long = ???

  override def getEvents(firstRecordId: Long, maxRecords: Int): util.List[ProvenanceEventRecord] = ???

  override def eventBuilder(): ProvenanceEventBuilder = ???

  override def close(): Unit = ???

  override def getEvent(id: Long): ProvenanceEventRecord = ???
}
