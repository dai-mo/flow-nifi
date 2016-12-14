package org.dcs.nifi.repo

import java.lang.{Iterable, Long}
import java.time.Instant
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import io.getquill.{CassandraSyncContext, SnakeCase}
import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.events.EventReporter
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission
import org.apache.nifi.provenance.search.{Query, QueryResult, QuerySubmission, SearchableField}
import org.apache.nifi.provenance._
import org.apache.nifi.util.NiFiProperties
import org.dcs.nifi.{FlowDataContent, FlowDataProvenance}

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 13.12.16.
  */
class CassandraProvenanceRepository extends ProvenanceRepository {

  private val ctx = new CassandraSyncContext[SnakeCase]("cassandra")


  private val properties: NiFiProperties = NiFiProperties.getInstance()

  private val  searchableFields: util.List[SearchableField] =
    SearchableFieldParser.extractSearchableFields(properties.getProperty(NiFiProperties.PROVENANCE_INDEXED_FIELDS), true)
  private val searchableAttributes: util.List[SearchableField] =
    SearchableFieldParser.extractSearchableFields(properties.getProperty(NiFiProperties.PROVENANCE_INDEXED_ATTRIBUTES), false)


  override def getSearchableFields: util.List[SearchableField] = searchableFields

  override def retrieveLineageSubmission(lineageIdentifier: String, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def retrieveQuerySubmission(queryIdentifier: String, user: NiFiUser): QuerySubmission =
    throw new UnsupportedOperationException()

  override def submitLineageComputation(flowFileUuid: String, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def submitLineageComputation(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def submitExpandParents(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def getProvenanceEventRepository: ProvenanceEventRepository = this

  override def initialize(eventReporter: EventReporter, authorizer: Authorizer, resourceFactory: ProvenanceAuthorizableFactory): Unit = {}

  override def getEvents(firstRecordId: Long, maxRecords: Int, user: NiFiUser): util.List[ProvenanceEventRecord] = {
    import ctx._

    val provQuery = quote(query[FlowDataProvenance].filter(fdp => fdp.eventId >= lift(firstRecordId) && fdp.eventId < lift(firstRecordId + maxRecords)))
    ctx.run(provQuery).map(_.toProvenanceEventRecord()).asJava
  }

  override def submitExpandChildren(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def getEvent(id: Long, user: NiFiUser): ProvenanceEventRecord = {
    import ctx._

    val provQuery = quote(query[FlowDataProvenance].filter(fdp => fdp.eventId == lift(id)))
    ctx.run(provQuery).map(_.toProvenanceEventRecord()).head
  }

  override def submitQuery(query: Query, user: NiFiUser): QuerySubmission = {
    val submissionTime = Date.from(Instant.now())

    import ctx._

    val provQuery = searchTermQuery(query)
    val result = run(provQuery).map(_.toProvenanceEventRecord())
    val qr = new DbQueryResult(result, "", 1L)

    new DbQuerySubmission(query, user.getIdentity, submissionTime, qr)
  }

  private def searchTermQuery(query: Query): ctx.Quoted[ctx.Query[FlowDataProvenance]] = {
    import ctx._

    quote {
      var ctxQuery = query[FlowDataProvenance]
      query.getSearchTerms.asScala.foreach(st => {
        val sfid = st.getSearchableField.getIdentifier
        sfid match {
          case SearchableFields.EventType.getIdentifier => ctxQuery = ctxQuery.filter(_.eventType == lift(sfid))
          case SearchableFields.FlowFileUUID.getIdentifier => ctxQuery = ctxQuery.filter(_.flowFileUuid == lift(sfid))
          case SearchableFields.ComponentID.getIdentifier => ctxQuery = ctxQuery.filter(_.componentId == lift(sfid))
          case SearchableFields.Relationship.getIdentifier => ctxQuery = ctxQuery.filter(_.relationship == lift(sfid))
        }
      })

      ctxQuery
    }
  }

  override def getSearchableAttributes: util.List[SearchableField] = searchableAttributes

  override def registerEvent(event: ProvenanceEventRecord): Unit = ???

  private def eventId(): Long = {
    import ctx._

    val dataQuery = quote(query[FlowDataContent].filter(p => p.id == lift(contentClaim.getResourceClaim.getId)))
    val result = ctx.run(dataQuery)
  }

  override def registerEvents(events: Iterable[ProvenanceEventRecord]): Unit = ???

  override def getMaxEventId: Long = ???

  override def getEvents(firstRecordId: Long, maxRecords: Int): util.List[ProvenanceEventRecord] = getEvents(firstRecordId, maxRecords, null)

  override def eventBuilder(): ProvenanceEventBuilder = ???

  override def close(): Unit = ctx.close()

  override def getEvent(id: Long): ProvenanceEventRecord = getEvent(id, null)
}

class DbQuerySubmission(query: Query,
                        submitterId: String,
                        submissionTime: Date,
                        queryResult: QueryResult) extends QuerySubmission {

  override def getQuery: Query = query

  override def cancel(): Unit = {}

  override def isCanceled: Boolean = false

  override def getQueryIdentifier: String = query.getIdentifier

  override def getSubmissionTime: Date = submissionTime

  override def getResult: QueryResult = queryResult

  override def getSubmitterIdentity: String = submitterId
}

class DbQueryResult(matchingEvents: List[ProvenanceEventRecord],
                    error: String,
                    queryTime: Long) extends QueryResult {

  override def isFinished: Boolean = true

  override def getExpiration: Date = Date.from(Instant.parse("2100-12-03T10:15:30.00Z"))

  override def getTotalHitCount: Long = matchingEvents.size.toLong

  override def getError: String = error

  override def getPercentComplete: Int = 100

  override def getQueryTime: Long = queryTime

  override def getMatchingEvents: util.List[ProvenanceEventRecord] = matchingEvents.asJava
}
