package org.dcs.flow.nifi

import java.time.{LocalDateTime, ZoneId}
import java.util.Date

import org.apache.nifi.web.api.entity.ProvenanceEntity
import org.dcs.api.error.{ErrorConstants, RESTException}
import org.dcs.api.service.{Provenance, ProvenanceApiService}

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import org.dcs.commons.JsonSerializerImplicits._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by cmathew on 12/08/16.
  */


class NifiProvenanceApi extends NifiProvenanceClient with NifiApiConfig

object NifiProvenanceClient {
  val ProvenanceQueryMaxTries = 10
  val ProvenancePath = "/provenance"

  def provenanceOutput(provenanceEventId: String) = {
    "/provenance-events/" + provenanceEventId + "/content/output"
  }

  def now: LocalDateTime  = LocalDateTime.now() // current date and time
  def midnight: LocalDateTime = now.toLocalDate.atStartOfDay()

  def defaultStart: Date = Date.from(midnight.atZone(ZoneId.systemDefault()).toInstant)
  def defaultEnd: Date  = Date.from(now.atZone(ZoneId.systemDefault()).toInstant)

  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProvenanceClient])
}

trait NifiProvenanceClient extends ProvenanceApiService with NifiBaseRestClient {
  import NifiProvenanceClient._

  override def provenance(processorId: String, maxResults: Int, startDate: Date = defaultStart, endDate: Date = defaultEnd): List[Provenance] = {
    // FIXME: This entire method needs to be moved to Future / Promise pattern

    val provenanceEntity = provenanceQueryRepeat(
      submitProvenanceQuery(processorId, maxResults, startDate, endDate),
      ProvenanceQueryMaxTries
    )

    try {
      if(!provenanceEntity.getProvenance.isFinished)
        throw new RESTException(ErrorConstants.DCS301)

      provenanceEntity.getProvenance.getResults.getProvenanceEvents.asScala.map(
        pe => Provenance(pe.getId,
          provenanceEntity.getProvenance.getId,
          pe.getClusterNodeId,
          getAsJson(path = provenanceOutput(pe.getEventId.toString),
            queryParams = params(pe.getClusterNodeId)))
      ).toList

    } finally {

      val delete = deleteAsJson(path = ProvenancePath + "/" + provenanceEntity.getProvenance.getId)
      logger.warn("Executing DELETE on " + ProvenancePath + "/" + provenanceEntity.getProvenance.getId)
    }

  }

  // ------ Helper Methods ------

  def submitProvenanceQuery(processorId: String, maxResults: Int, startDate: Date, endDate: Date): ProvenanceEntity =
    postAsJson(path = ProvenancePath,
      obj = ProcessorProvenanceSearchRequest(processorId,
        maxResults,
        startDate,
        endDate).toJson).
      toObject[ProvenanceEntity]

  def provenanceQueryRepeat(provenanceEntity: ProvenanceEntity, triesLeft: Int): ProvenanceEntity = triesLeft match {
    case 0 => provenanceEntity
    case _ => provenanceEntity.getProvenance.isFinished match {
      case java.lang.Boolean.TRUE => provenanceEntity
      case java.lang.Boolean.FALSE => provenanceQueryRepeat(
        provenanceQuery(provenanceEntity.getProvenance.getId, provenanceEntity.getProvenance.getRequest.getClusterNodeId),
        triesLeft - 1
      )
    }
  }

  def provenanceQuery(provenanceEntityId: String, clusterNodeId: String): ProvenanceEntity =
    getAsJson(path = ProvenancePath + "/" + provenanceEntityId,
      queryParams = params(clusterNodeId)).toObject[ProvenanceEntity]

  def params(clusterNodeId: String): Map[String, String] = {
    if(clusterNodeId == null)
      Map()
    else
      Map("clusterNodeId" -> clusterNodeId)
  }
}
