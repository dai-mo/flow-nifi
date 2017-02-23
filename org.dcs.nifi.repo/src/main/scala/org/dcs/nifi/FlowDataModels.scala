package org.dcs.nifi

/**
  * Created by cmathew on 06.12.16.
  */


import java.util
import java.util.UUID

import org.apache.nifi.provenance.search.Query
import org.apache.nifi.provenance.{ProvenanceEventRecord, ProvenanceEventType, SearchableFields}
import org.dcs.api.data.FlowDataProvenance

import scala.collection.JavaConverters._


object FlowProvenanceEventRecord  {
  import org.dcs.api.data.FlowData._

  def toFlowDataProvenance(per: ProvenanceEventRecord, eventId: Option[Double]): FlowDataProvenance = {
    val eid = eventId.getOrElse(per.getEventId.toDouble)
    val previousFileSize = if(per.getPreviousFileSize == null) 0 else per.getPreviousFileSize.toDouble
    FlowDataProvenance(UUID.randomUUID().toString,
      eid,
      per.getEventTime,
      per.getFlowFileEntryDate,
      per.getLineageStartDate,
      per.getFileSize.toDouble,
      previousFileSize,
      per.getEventDuration,
      per.getEventType.name(),
      mapToString(per.getAttributes.asScala.toMap),
      mapToString(per.getPreviousAttributes.asScala.toMap),
      mapToString(per.getUpdatedAttributes.asScala.toMap),
      per.getComponentId,
      per.getComponentType,
      Option(per.getTransitUri).getOrElse(""),
      Option(per.getSourceSystemFlowFileIdentifier).getOrElse(""),
      per.getFlowFileUuid,
      listToString(per.getParentUuids.asScala.toList),
      listToString(per.getChildUuids.asScala.toList),
      Option(per.getAlternateIdentifierUri).getOrElse(""),
      Option(per.getDetails).getOrElse(""),
      Option(per.getRelationship).getOrElse(""),
      Option(per.getSourceQueueIdentifier).getOrElse(""),
      per.getContentClaimIdentifier,
      Option(per.getPreviousContentClaimIdentifier).getOrElse(""))
  }


  def apply(fdp: FlowDataProvenance): ProvenanceEventRecord =
    new FlowProvenanceEventRecord(fdp)
}

class FlowProvenanceEventRecord(flowDataProvenance: FlowDataProvenance) extends ProvenanceEventRecord {
  import org.dcs.api.data.FlowData._

  override def getRelationship: String = flowDataProvenance.relationship

  override def getDetails: String = flowDataProvenance.details

  override def getAttributes: util.Map[String, String] =
    stringToMap(flowDataProvenance.attributes).asJava

  override def getParentUuids: util.List[String] =
    stringToList(flowDataProvenance.parentUuids).asJava


  override def getFlowFileEntryDate: Long = flowDataProvenance.flowFileEntryDate.toLong

  override def getAlternateIdentifierUri: String = flowDataProvenance.alternateIdentifierUri

  override def getChildUuids: util.List[String] =
    stringToList(flowDataProvenance.childUuids).asJava

  override def getContentClaimContainer: String = ""

  override def getFlowFileUuid: String = flowDataProvenance.flowFileUuid

  override def getComponentId: String = flowDataProvenance.componentId

  override def getPreviousContentClaimIdentifier: String = flowDataProvenance.previousContentClaimIdentifier

  override def getEventType: ProvenanceEventType = {
    flowDataProvenance.eventType match {
      case "ADDINFO" => ProvenanceEventType.ADDINFO
      case "ATTRIBUTES_MODIFIED" => ProvenanceEventType.ATTRIBUTES_MODIFIED
      case "CLONE" => ProvenanceEventType.CLONE
      case "CONTENT_MODIFIED" => ProvenanceEventType.CONTENT_MODIFIED
      case "CREATE" => ProvenanceEventType.CREATE
      case "DOWNLOAD" => ProvenanceEventType.DOWNLOAD
      case "DROP" => ProvenanceEventType.DROP
      case "EXPIRE" => ProvenanceEventType.EXPIRE
      case "FETCH" => ProvenanceEventType.FETCH
      case "FORK" => ProvenanceEventType.FORK
      case "JOIN" => ProvenanceEventType.JOIN
      case "RECEIVE" => ProvenanceEventType.RECEIVE
      case "REPLAY" => ProvenanceEventType.REPLAY
      case "ROUTE" => ProvenanceEventType.ROUTE
      case "SEND" => ProvenanceEventType.SEND
      case "UNKNOWN" => ProvenanceEventType.UNKNOWN
      case _ => ProvenanceEventType.UNKNOWN
    }
  }

  override def getEventId: Long = flowDataProvenance.eventId.toLong

  override def getEventDuration: Long = flowDataProvenance.eventDuration.toLong

  override def getPreviousFileSize: java.lang.Long = flowDataProvenance.previousFileSize.toLong

  override def getPreviousAttributes: util.Map[String, String] =
    stringToMap(flowDataProvenance.previousAttributes).asJava

  override def getSourceSystemFlowFileIdentifier: String = flowDataProvenance.sourceSystemFlowFileIdentifier

  override def getContentClaimSection: String = ""

  override def getContentClaimOffset: java.lang.Long = 0L

  override def getFileSize: Long = flowDataProvenance.fileSize.toLong

  override def getContentClaimIdentifier: String = flowDataProvenance.contentClaimIdentifier

  override def getPreviousContentClaimOffset: java.lang.Long = 0L

  override def getUpdatedAttributes: util.Map[String, String] =
    stringToMap(flowDataProvenance.updatedAttributes).asJava

  override def getPreviousContentClaimContainer: String = ""

  override def getPreviousContentClaimSection: String = ""

  override def getComponentType: String = flowDataProvenance.componentType

  override def getTransitUri: String = flowDataProvenance.transitUri

  override def getEventTime: Long = flowDataProvenance.eventTime.toLong

  override def getLineageStartDate: Long = flowDataProvenance.lineageStartEntryDate.toLong

  override def getSourceQueueIdentifier: String = flowDataProvenance.sourceQueueIdentifier

}

case class FlowId(name: String, latestId: Long)

object SearchableIds {
  def apply(searchQuery: Query): SearchableIds = {
    val searchableIds: SearchableIds = new SearchableIds()
    searchQuery.getSearchTerms.asScala.foreach(st => {
      val sfid = st.getSearchableField
      sfid match {
        case SearchableFields.EventType => searchableIds.eventType = Some(st.getValue)
        case SearchableFields.FlowFileUUID => searchableIds.flowFileUuid = Some(st.getValue)
        case SearchableFields.ComponentID => searchableIds.componentId = Some(st.getValue)
        case SearchableFields.Relationship => searchableIds.relationship = Some(st.getValue)
      }
    })
    searchableIds
  }
}
case class SearchableIds(var eventType: Option[String] = None,
                         var flowFileUuid: Option[String] = None,
                         var componentId: Option[String] = None,
                         var relationship: Option[String] = None) {
  def isEmpty: Boolean = eventType.isEmpty && flowFileUuid.isEmpty && componentId.isEmpty && relationship.isEmpty
}