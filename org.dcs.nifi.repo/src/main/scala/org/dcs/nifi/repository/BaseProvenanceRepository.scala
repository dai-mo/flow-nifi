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
import org.dcs.api.data.FlowDataProvenance
import org.dcs.data.IntermediateResultsAdapter
import org.dcs.data.slick.BigTables
import org.dcs.nifi._

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._



/**
  * Created by cmathew on 13.12.16.
  */

object BaseProvenanceRepository {
  val QueryMap = new ConcurrentHashMap[String, Query]()
}

trait ManageRepository {
  def purge(): Unit
}

class BaseProvenanceRepository(ira: IntermediateResultsAdapter) extends ProvenanceRepository with ManageRepository {

  private val timeout = 60 seconds

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
    Await.result(ira.getProvenanceEventsByEventId(firstRecordId, maxRecords), timeout).
      map(FlowProvenanceEventRecord(_)).asJava
  }

  override def submitExpandChildren(eventId: Long, user: NiFiUser): ComputeLineageSubmission =
    throw new UnsupportedOperationException()

  override def getEvent(id: Long, user: NiFiUser): ProvenanceEventRecord = {
    Await.result(ira.getProvenanceEventByEventId(id), timeout).
      map(FlowProvenanceEventRecord(_)).orNull
  }

  override def submitQuery(searchQuery: Query, user: NiFiUser): QuerySubmission = {

    val startDate = Date.from(Instant.now())
    val qr = new DbQueryResult(Nil, "", 1L, 0)
    BaseProvenanceRepository.QueryMap.put(searchQuery.getIdentifier, searchQuery)
    new DbQuerySubmission(searchQuery, "nifi_user", startDate, qr)
  }

  private def searchTermQuery(searchQuery: Query): List[ProvenanceEventRecord] = {


    val searchableIds = SearchableIds(searchQuery)

    var records: List[BigTables.BigFlowDataProvenanceRow] = Nil

    if(searchableIds.isEmpty) {
      records = Await.result(ira.getProvenanceEvents(searchQuery.getMaxResults), timeout)
    } else {

      if (searchableIds.eventType.isDefined)
        records =
          Await.result(
            ira.getProvenanceEventsByEventType(searchableIds.eventType.get,
              searchQuery.getMaxResults),
            timeout) ++ records

      if (searchableIds.flowFileUuid.isDefined)
        records =
          Await.result(
            ira.getProvenanceEventsByFlowFileUuid(searchableIds.flowFileUuid.get,
              searchQuery.getMaxResults),
            timeout) ++ records

      if (searchableIds.componentId.isDefined)
        records =
          Await.result(
            ira.getProvenanceEventsByComponentId(searchableIds.componentId.get,
              searchQuery.getMaxResults),
            timeout) ++ records

      if (searchableIds.relationship.isDefined)
        records =
          Await.result(
            ira.getProvenanceEventsByRelationship(searchableIds.relationship.get,
              searchQuery.getMaxResults),
            timeout) ++ records
    }
    records.
      groupBy(_.eventId).
      map(_._2.head).toList.
      sortBy(_.eventTime).
      take(searchQuery.getMaxResults).
      map(FlowProvenanceEventRecord(_))
  }

  override def getSearchableAttributes: util.List[SearchableField] = searchableAttributes

  override def registerEvent(event: ProvenanceEventRecord): Unit =
    Await.result(ira.createProvenance(FlowProvenanceEventRecord.toFlowDataProvenanceRow(event, Some(0))), timeout)



  override def registerEvents(events: Iterable[ProvenanceEventRecord]): Unit = {
    events.asScala.foreach(registerEvent)
  }

  def getMaxEventId: java.lang.Long = Await.result(ira.getProvenanceMaxEventId(), timeout).getOrElse(-1)

  override def getEvents(firstRecordId: Long, maxRecords: Int): util.List[ProvenanceEventRecord] = getEvents(firstRecordId, maxRecords, null)

  override def eventBuilder(): ProvenanceEventBuilder = new Builder

  override def close(): Unit = {}

  override def getEvent(id: Long): ProvenanceEventRecord = getEvent(id, null)

  override def purge(): Unit = Await.result(ira.purgeProvenance(), timeout)
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

