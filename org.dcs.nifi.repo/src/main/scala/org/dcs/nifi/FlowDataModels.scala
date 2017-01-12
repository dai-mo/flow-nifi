package org.dcs.nifi

/**
  * Created by cmathew on 06.12.16.
  */


import java.util
import java.util.{Date, UUID}

import org.apache.nifi.provenance.search.Query
import org.apache.nifi.provenance.{ProvenanceEventRecord, ProvenanceEventType, SearchableFields}
import org.dcs.nifi.repository.DcsContentClaim

import scala.collection.JavaConverters._

case class AutoIds(name: String, id: Double) {
  override def equals(that: Any): Boolean = that match {
    case AutoIds(thatName, thatId) =>
      thatName == this.name
    case _ => false
  }
}

object FlowDataContent {

  def apply(claim: DcsContentClaim, data: Array[Byte]): FlowDataContent = {
    new FlowDataContent(claim.getResourceClaim.getId, 0, claim.getTimestamp, data)
  }
}

case class FlowDataContent(id: String, claimCount: Int, timestamp: Date, data: Array[Byte]) {
  override def equals(that: Any): Boolean = that match {
    case FlowDataContent(thatId, thatClaimCount, thatTimestamp, thatData) =>
      thatId == this.id &&
        thatClaimCount == this.claimCount &&
        thatTimestamp == this.timestamp &&
        thatData.deep == this.data.deep
    case _ => false
  }
}


case class FlowDataProvenance(id: String,
                              eventId: Double,
                              eventTime: Double,
                              flowFileEntryDate: Double,
                              lineageStartEntryDate: Double,
                              fileSize: Double,
                              previousFileSize: Double,
                              eventDuration: Double,
                              eventType: String,
                              attributes: String,
                              previousAttributes:String,
                              updatedAttributes: String,
                              componentId: String,
                              componentType: String,
                              transitUri: String,
                              sourceSystemFlowFileIdentifier: String,
                              flowFileUuid: String,
                              parentUuids: String,
                              childUuids: String,
                              alternateIdentifierUri: String,
                              details: String,
                              relationship: String,
                              sourceQueueIdentifier: String,
                              contentClaimIdentifier: String,
                              previousContentClaimIdentifier: String) {
  override def equals(that: Any): Boolean = {
    if(that == null)
      false
    else if(that.isInstanceOf[FlowDataProvenance]) {
      val thatFdp = that.asInstanceOf[FlowDataProvenance]
      this.eventId == thatFdp.eventId
    } else
      false
  }

  def toProvenanceEventRecord(): ProvenanceEventRecord = {
    new FlowProvenanceEventRecord(this)
  }

}

object FlowProvenanceEventRecord {
  def toFlowDataProvenance(per: ProvenanceEventRecord, eventId: Option[Double]): FlowDataProvenance = {
    val eid = eventId.getOrElse(per.getEventId.toDouble)
    FlowDataProvenance(UUID.randomUUID().toString,
      eid,
      per.getEventTime,
      per.getFlowFileEntryDate,
      per.getLineageStartDate,
      per.getFileSize.toDouble,
      per.getPreviousFileSize.toDouble,
      per.getEventDuration,
      per.getEventType.name(),
      mapToString(per.getAttributes),
      mapToString(per.getPreviousAttributes),
      mapToString(per.getUpdatedAttributes),
      per.getComponentId,
      per.getComponentType,
      per.getTransitUri,
      per.getSourceSystemFlowFileIdentifier,
      per.getFlowFileUuid,
      listToString(per.getParentUuids),
      listToString(per.getChildUuids),
      per.getAlternateIdentifierUri,
      per.getDetails,
      per.getRelationship,
      per.getSourceQueueIdentifier,
      per.getContentClaimIdentifier,
      per.getPreviousContentClaimIdentifier)
  }

  def mapToString[K, V](map: util.Map[K, V]): String = {
    if(map == null || map.isEmpty)
      ""
    else
      map.asScala.toList.map(x => x._1 + ":" + x._2).mkString(",")
  }

  def listToString[T](list: util.List[T]): String = {
    if(list == null || list.isEmpty)
      ""
    else
      list.asScala.mkString(",")
  }
}

class FlowProvenanceEventRecord(flowDataProvenance: FlowDataProvenance) extends ProvenanceEventRecord {
  override def getRelationship: String = flowDataProvenance.relationship

  override def getDetails: String = flowDataProvenance.details

  override def getAttributes: util.Map[String, String] =
    flowDataProvenance.attributes.split(",").map(a => {
      val attr = a.split(":")
      attr.head -> attr.tail.head
    }).toMap.asJava


  override def getParentUuids: util.List[String] =
    flowDataProvenance.parentUuids.split(",").toList.asJava

  override def getFlowFileEntryDate: Long = flowDataProvenance.flowFileEntryDate.toLong

  override def getAlternateIdentifierUri: String = flowDataProvenance.alternateIdentifierUri

  override def getChildUuids: util.List[String] = flowDataProvenance.childUuids.split(",").toList.asJava

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
    flowDataProvenance.previousAttributes.split(",").map(a => {
      val attr = a.split(":")
      attr.head -> attr.tail.head
    }).toMap.asJava

  override def getSourceSystemFlowFileIdentifier: String = flowDataProvenance.sourceSystemFlowFileIdentifier

  override def getContentClaimSection: String = ""

  override def getContentClaimOffset: java.lang.Long = 0L

  override def getFileSize: Long = flowDataProvenance.fileSize.toLong

  override def getContentClaimIdentifier: String = flowDataProvenance.contentClaimIdentifier

  override def getPreviousContentClaimOffset: java.lang.Long = 0L

  override def getUpdatedAttributes: util.Map[String, String] =
    flowDataProvenance.updatedAttributes.split(",").map(a => {
      val attr = a.split(":")
      attr.head -> attr.tail.head
    }).toMap.asJava

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
                         var relationship: Option[String] = None)