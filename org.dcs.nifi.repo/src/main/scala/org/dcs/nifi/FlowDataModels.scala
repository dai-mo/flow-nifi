package org.dcs.nifi

/**
  * Created by cmathew on 06.12.16.
  */


import java.util
import java.util.UUID

import org.apache.nifi.provenance.search.Query
import org.apache.nifi.provenance.{ProvenanceEventRecord, ProvenanceEventType, SearchableFields}
import org.dcs.api.data.{FlowData, FlowDataProvenance}
import org.dcs.api.processor.Attributes
import org.dcs.data.slick.BigTables

import scala.collection.JavaConverters._


object FlowProvenanceEventRecord  {
  import org.dcs.api.data.FlowData._

  def toFlowDataProvenance(per: ProvenanceEventRecord, eventId: Option[Double]): FlowDataProvenance = {
    val eid = eventId.getOrElse(per.getEventId.toDouble)
    val previousFileSize = if(per.getPreviousFileSize == null) 0 else per.getPreviousFileSize.toDouble
    val scUpdatedAttributes = per.getUpdatedAttributes.asScala.toMap

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
      Option(per.getRelationship).fold(scUpdatedAttributes.get(Attributes.RelationshipAttributeKey))(Option(_)).getOrElse(""),
      Option(per.getSourceQueueIdentifier).getOrElse(""),
      per.getContentClaimIdentifier,
      Option(per.getPreviousContentClaimIdentifier).getOrElse(""))
  }



  def toFlowDataProvenanceRow(per: ProvenanceEventRecord, eventId: Option[Double]): BigTables.BigFlowDataProvenanceRow = {
    def cleanAttributes(attributes: Map[String, String]): String = {
      mapToString(attributes - Attributes.RelationshipAttributeKey - Attributes.ComponentTypeAttributeKey)
    }

    val eid = eventId.getOrElse(per.getEventId.toDouble)
    val previousFileSize = if(per.getPreviousFileSize == null) 0 else per.getPreviousFileSize.toDouble

    val updatedAttributes = per.getUpdatedAttributes.asScala.toMap
    val updatedAttributesStr: Option[String] = Option(cleanAttributes(updatedAttributes))

    var relationship = Option(per.getRelationship)
    var componentType = updatedAttributes.get(Attributes.ComponentTypeAttributeKey)

    if(relationship.isEmpty) {
      relationship = updatedAttributes.get(Attributes.RelationshipAttributeKey)
    }

    val attributes = per.getAttributes.asScala.toMap
    val attributesStr: Option[String] = Option(cleanAttributes(attributes))

    if(relationship.isEmpty) {
      relationship = attributes.get(Attributes.RelationshipAttributeKey)
    }

    if(componentType.isEmpty) {
      componentType = attributes.get(Attributes.ComponentTypeAttributeKey)
    }

    if(componentType.isEmpty) {
      componentType = Option(per.getComponentType)
    }

    val previousAttributes = per.getPreviousAttributes.asScala.toMap
    val previousAttributesStr: Option[String] = Option(cleanAttributes(previousAttributes))

    BigTables.BigFlowDataProvenanceRow(UUID.randomUUID().toString,
      eid.toLong,
      Option(per.getEventTime),
      Option(per.getFlowFileEntryDate),
      Option(per.getLineageStartDate),
      Option(per.getFileSize.toDouble),
      Option(previousFileSize),
      Option(per.getEventDuration),
      Option(per.getEventType.name()),
      attributesStr,
      previousAttributesStr,
      updatedAttributesStr,
      Option(per.getComponentId),
      componentType,
      Option(per.getTransitUri),
      Option(per.getSourceSystemFlowFileIdentifier),
      Option(per.getFlowFileUuid),
      Option(listToString(per.getParentUuids.asScala.toList)),
      Option(listToString(per.getChildUuids.asScala.toList)),
      Option(per.getAlternateIdentifierUri),
      Option(per.getDetails),
      relationship,
      Option(per.getSourceQueueIdentifier),
      Option(per.getContentClaimIdentifier),
      Option(per.getPreviousContentClaimIdentifier))
  }


  def apply(fdp: BigTables.BigFlowDataProvenanceRow): ProvenanceEventRecord =
    new FlowProvenanceEventRecord(fdp)
}

class FlowProvenanceEventRecord(flowDataProvenance: BigTables.BigFlowDataProvenanceRow) extends ProvenanceEventRecord {
  import org.dcs.api.data.FlowData._

  override def getRelationship: String = flowDataProvenance.relationship.orNull

  override def getDetails: String = flowDataProvenance.details.orNull

  override def getAttributes: util.Map[String, String] =
    stringToMap(flowDataProvenance.attributes.get).asJava

  override def getParentUuids: util.List[String] =
    stringToList(flowDataProvenance.parentUuids.get).asJava


  override def getFlowFileEntryDate: Long = flowDataProvenance.flowFileEntryDate.getOrElse(-1.toDouble).toLong

  override def getAlternateIdentifierUri: String = flowDataProvenance.alternateIdentifierUri.orNull

  override def getChildUuids: util.List[String] =
    stringToList(flowDataProvenance.childUuids.get).asJava

  override def getContentClaimContainer: String = ""

  override def getFlowFileUuid: String = flowDataProvenance.flowFileUuid.orNull

  override def getComponentId: String = flowDataProvenance.componentId.orNull

  override def getPreviousContentClaimIdentifier: String = flowDataProvenance.previousContentClaimIdentifier.orNull

  override def getEventType: ProvenanceEventType = {
    flowDataProvenance.eventType.get match {
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

  override def getEventDuration: Long = flowDataProvenance.eventDuration.getOrElse(-1.toDouble).toLong

  override def getPreviousFileSize: java.lang.Long = flowDataProvenance.previousFileSize.getOrElse(-1.toDouble).toLong

  override def getPreviousAttributes: util.Map[String, String] =
    stringToMap(flowDataProvenance.previousAttributes.get).asJava

  override def getSourceSystemFlowFileIdentifier: String = flowDataProvenance.sourceSystemFlowFileIdentifier.orNull

  override def getContentClaimSection: String = ""

  override def getContentClaimOffset: java.lang.Long = 0L

  override def getFileSize: Long = flowDataProvenance.fileSize.get.toLong

  override def getContentClaimIdentifier: String = flowDataProvenance.contentClaimIdentifier.orNull

  override def getPreviousContentClaimOffset: java.lang.Long = 0L

  override def getUpdatedAttributes: util.Map[String, String] =
    stringToMap(flowDataProvenance.updatedAttributes.get).asJava

  override def getPreviousContentClaimContainer: String = ""

  override def getPreviousContentClaimSection: String = ""

  override def getComponentType: String = flowDataProvenance.componentType.orNull

  override def getTransitUri: String = flowDataProvenance.transitUri.orNull

  override def getEventTime: Long = flowDataProvenance.eventTime.getOrElse(-1.toDouble).toLong

  override def getLineageStartDate: Long = flowDataProvenance.lineageStartEntryDate.getOrElse(-1.toDouble).toLong

  override def getSourceQueueIdentifier: String = flowDataProvenance.sourceQueueIdentifier.orNull

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