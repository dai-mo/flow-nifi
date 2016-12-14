package org.dcs.nifi

/**
  * Created by cmathew on 06.12.16.
  */

import java.lang.Long
import java.util
import java.util.{Date, UUID}

import org.apache.nifi.provenance.{ProvenanceEventRecord, ProvenanceEventType}
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

case class FlowDataProvenance(eventId: Long,
                              eventTime: Long,
                              flowFileEntryDate: Long,
                              lineageStartEntryDate: Long,
                              fileSize: Long,
                              previousFileSize: Long,
                              eventDuration: Long,
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

  override def getFlowFileEntryDate: Long = flowDataProvenance.flowFileEntryDate

  override def getAlternateIdentifierUri: String = flowDataProvenance.alternateIdentifierUri

  override def getChildUuids: util.List[String] = flowDataProvenance.childUuids.split(",").toList.asJava

  override def getContentClaimContainer: String = ""

  override def getFlowFileUuid: String = flowDataProvenance.flowFileUuid

  override def getComponentId: String = flowDataProvenance.componentId

  override def getPreviousContentClaimIdentifier: String = flowDataProvenance.previousContentClaimIdentifier

  override def getEventType: ProvenanceEventType = {
    flowDataProvenance.eventType match {
      case "CREATE" => ProvenanceEventType.ADDINFO
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
      case "REPLAY" => ProvenanceEventType.ROUTE
      case "SEND" => ProvenanceEventType.SEND
      case "UNKNOWN" => ProvenanceEventType.UNKNOWN
      case _ => ProvenanceEventType.UNKNOWN
    }
  }

  override def getEventId: Long = flowDataProvenance.eventId

  override def getEventDuration: Long = flowDataProvenance.eventDuration

  override def getPreviousFileSize: Long = flowDataProvenance.previousFileSize

  override def getPreviousAttributes: util.Map[String, String] =
    flowDataProvenance.previousAttributes.split(",").map(a => {
      val attr = a.split(":")
      attr.head -> attr.tail.head
    }).toMap.asJava

  override def getSourceSystemFlowFileIdentifier: String = flowDataProvenance.sourceSystemFlowFileIdentifier

  override def getContentClaimSection: String = ""

  override def getContentClaimOffset: Long = 0L

  override def getFileSize: Long = flowDataProvenance.fileSize

  override def getContentClaimIdentifier: String = flowDataProvenance.contentClaimIdentifier

  override def getPreviousContentClaimOffset: Long = 0L

  override def getUpdatedAttributes: util.Map[String, String] =
    flowDataProvenance.updatedAttributes.split(",").map(a => {
      val attr = a.split(":")
      attr.head -> attr.tail.head
    }).toMap.asJava

  override def getPreviousContentClaimContainer: String = ""

  override def getPreviousContentClaimSection: String = ""

  override def getComponentType: String = flowDataProvenance.componentType

  override def getTransitUri: String = flowDataProvenance.transitUri

  override def getEventTime: Long = flowDataProvenance.eventTime

  override def getLineageStartDate: Long = flowDataProvenance.lineageStartEntryDate

  override def getSourceQueueIdentifier: String = flowDataProvenance.sourceQueueIdentifier
}