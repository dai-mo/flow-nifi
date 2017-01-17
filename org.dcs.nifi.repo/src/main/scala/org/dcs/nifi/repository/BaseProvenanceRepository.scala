package org.dcs.nifi.repository

import java.lang.Iterable
import java.time.Instant
import java.util
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.events.EventReporter
import org.apache.nifi.provenance.StandardProvenanceEventRecord.Builder
import org.apache.nifi.provenance._
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission
import org.apache.nifi.provenance.search.{Query, QueryResult, QuerySubmission, SearchableField}
import org.apache.nifi.util.NiFiProperties
import org.dcs.nifi._

import scala.collection.JavaConverters._


/**
  * Created by cmathew on 13.12.16.
  */

object BaseProvenanceRepository {
  val QueryMap = new ConcurrentHashMap[String, Query]()
}

trait ManageRepository {
  def purge(): Unit
}

abstract class BaseProvenanceRepository extends ProvenanceRepository with ManageRepository {


  protected val ctx = new QuillContext

  private val properties: NiFiProperties = NiFiProperties.getInstance()

  private val  searchableFields: util.List[SearchableField] =
    SearchableFieldParser.extractSearchableFields(properties.getProperty(NiFiProperties.PROVENANCE_INDEXED_FIELDS), true)
  private val searchableAttributes: util.List[SearchableField] =
    SearchableFieldParser.extractSearchableFields(properties.getProperty(NiFiProperties.PROVENANCE_INDEXED_ATTRIBUTES), false)


  override def getSearchableFields: util.List[SearchableField] = searchableFields

  override def retrieveLineageSubmission(lineageIdentifier: String, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def retrieveQuerySubmission(queryIdentifier: String, user: NiFiUser): QuerySubmission = {
    val searchQuery = BaseProvenanceRepository.QueryMap.get(queryIdentifier)
    if(searchQuery == null)
      null
    else {
      val startDate = Date.from(Instant.now())
      val records = searchTermQuery(searchQuery)
      val endDate = Date.from(Instant.now())
      searchQuery.setEndDate(endDate)

      if (records.nonEmpty) {
        val minFileSize = records.min(Ordering.by((per: ProvenanceEventRecord) => per.getFileSize))
        searchQuery.setMinFileSize(minFileSize.toString)
        val maxFileSize = records.max(Ordering.by((per: ProvenanceEventRecord) => per.getFileSize))
        searchQuery.setMinFileSize(maxFileSize.toString)
      }
      val qr = new DbQueryResult(records, "", 1L, 100)
      BaseProvenanceRepository.QueryMap.remove(queryIdentifier)
      new DbQuerySubmission(searchQuery, "nifi_user", startDate, qr)
    }
  }


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

    val provQuery = quote(query[FlowDataProvenance].filter(fdp =>
      fdp.eventId >= lift(firstRecordId.toDouble) && fdp.eventId < lift(firstRecordId.toDouble + maxRecords)
    ))

    ctx.run(provQuery).map(_.toProvenanceEventRecord()).sortBy(_.getEventId).asJava
  }

  override def submitExpandChildren(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def getEvent(id: Long, user: NiFiUser): ProvenanceEventRecord = {
    import ctx._

    val provQuery = quote(query[FlowDataProvenance].filter(fdp => fdp.eventId == lift(id.toDouble)))
    ctx.run(provQuery).map(_.toProvenanceEventRecord()).head
  }

  override def submitQuery(searchQuery: Query, user: NiFiUser): QuerySubmission = {

    val startDate = Date.from(Instant.now())
    val qr = new DbQueryResult(Nil, "", 1L, 0)
    BaseProvenanceRepository.QueryMap.put(searchQuery.getIdentifier, searchQuery)
    new DbQuerySubmission(searchQuery, "nifi_user", startDate, qr)
  }

  private def searchTermQuery(searchQuery: Query): List[ProvenanceEventRecord] = {
    import ctx._

    val withFilter = quote {
      (g: FlowDataProvenance => Boolean) =>
        query[FlowDataProvenance].withFilter(g(_))
    }

    val searchableIds = SearchableIds(searchQuery)

    var records: List[ProvenanceEventRecord] = Nil

    if(searchableIds.isEmpty()) {
      val allRecords = ctx.run(quote {
        query[FlowDataProvenance]
          .take(lift(searchQuery.getMaxResults))
      })
      records = allRecords.map(_.toProvenanceEventRecord()) ++ records
    } else {
      if (searchableIds.eventType.isDefined)
        records = ctx.run(quote {
          withFilter((fdp: FlowDataProvenance) => fdp.eventType == lift(searchableIds.eventType.get))
            .take(lift(searchQuery.getMaxResults))
        }).map(_.toProvenanceEventRecord()) ++ records

      if (searchableIds.flowFileUuid.isDefined)
        records = ctx.run(quote {
          withFilter((fdp: FlowDataProvenance) => fdp.flowFileUuid == lift(searchableIds.flowFileUuid.get))
            .take(lift(searchQuery.getMaxResults))
        }).map(_.toProvenanceEventRecord()) ++ records

      if (searchableIds.componentId.isDefined)
        records = ctx.run(quote {
          withFilter((fdp: FlowDataProvenance) => fdp.componentId == lift(searchableIds.componentId.get))
            .take(lift(searchQuery.getMaxResults))
        }).map(_.toProvenanceEventRecord()) ++ records

      if (searchableIds.relationship.isDefined)
        records = ctx.run(quote {
          withFilter((fdp: FlowDataProvenance) => fdp.relationship == lift(searchableIds.relationship.get))
            .take(lift(searchQuery.getMaxResults))
        }).map(_.toProvenanceEventRecord()) ++ records
    }
    records.groupBy(_.getEventId).map(_._2.head).toList.sortBy(_.getEventId).take(searchQuery.getMaxResults)
  }

  override def getSearchableAttributes: util.List[SearchableField] = searchableAttributes

  override def registerEvent(event: ProvenanceEventRecord): Unit = {
    import ctx._

    val fdp: FlowDataProvenance = FlowProvenanceEventRecord.toFlowDataProvenance(event, Some(0))

    val eventInsert = quote(query[FlowDataProvenance].insert(_.id -> lift(fdp.id),
      _.eventTime -> lift(fdp.eventTime),
      _.flowFileEntryDate -> lift(fdp.flowFileEntryDate),
      _.lineageStartEntryDate -> lift(fdp.lineageStartEntryDate),
      _.fileSize -> lift(fdp.fileSize),
      _.previousFileSize -> lift(fdp.previousFileSize),
      _.eventDuration -> lift(fdp.eventDuration),
      _.eventType -> lift(fdp.eventType),
      _.attributes -> lift(fdp.attributes),
      _.previousAttributes -> lift(fdp.previousAttributes),
      _.updatedAttributes -> lift(fdp.updatedAttributes),
      _.componentId -> lift(fdp.componentId),
      _.componentType -> lift(fdp.componentType),
      _.transitUri -> lift(fdp.transitUri),
      _.sourceSystemFlowFileIdentifier -> lift(fdp.sourceSystemFlowFileIdentifier),
      _.flowFileUuid -> lift(fdp.flowFileUuid),
      _.parentUuids -> lift(fdp.parentUuids),
      _.childUuids -> lift(fdp.childUuids),
      _.alternateIdentifierUri -> lift(fdp.alternateIdentifierUri),
      _.details -> lift(fdp.details),
      _.relationship -> lift(fdp.relationship),
      _.sourceQueueIdentifier -> lift(fdp.sourceQueueIdentifier),
      _.contentClaimIdentifier -> lift(fdp.contentClaimIdentifier),
      _.previousContentClaimIdentifier -> lift(fdp.previousContentClaimIdentifier)))
    ctx.run(eventInsert)
  }


  override def registerEvents(events: Iterable[ProvenanceEventRecord]): Unit = {
    events.asScala.foreach(registerEvent)
  }

  def getMaxEventId: java.lang.Long

  override def getEvents(firstRecordId: Long, maxRecords: Int): util.List[ProvenanceEventRecord] = getEvents(firstRecordId, maxRecords, null)

  override def eventBuilder(): ProvenanceEventBuilder = new Builder

  override def close(): Unit = ctx.close()

  override def getEvent(id: Long): ProvenanceEventRecord = getEvent(id, null)

  override def purge(): Unit = {
    import ctx._

    val provenancePurge = quote {
      query[FlowDataProvenance]
        .delete
    }
    ctx.run(provenancePurge)

  }
}

class DbQuerySubmission(query: Query,
                        submitterId: String,
                        submissionTime: Date,
                        queryResult: QueryResult) extends QuerySubmission {

  override def getQuery: Query = query

  override def cancel(): Unit =  {}

  override def isCanceled: Boolean = false

  override def getQueryIdentifier: String = query.getIdentifier

  override def getSubmissionTime: Date = submissionTime

  override def getResult: QueryResult = queryResult

  override def getSubmitterIdentity: String = submitterId
}

class DbQueryResult(matchingEvents: List[ProvenanceEventRecord],
                    error: String,
                    queryTime: Long,
                    percentComplete: Int) extends QueryResult {

  override def isFinished: Boolean = true

  override def getExpiration: Date = Date.from(Instant.parse("2100-12-03T10:15:30.00Z"))

  override def getTotalHitCount = matchingEvents.size.toLong

  override def getError: String = error

  override def getPercentComplete: Int = percentComplete

  override def getQueryTime = queryTime

  override def getMatchingEvents: util.List[ProvenanceEventRecord] = matchingEvents.asJava
}

