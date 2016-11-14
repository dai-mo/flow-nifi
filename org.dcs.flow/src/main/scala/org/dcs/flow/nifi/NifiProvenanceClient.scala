package org.dcs.flow.nifi

import java.time.{LocalDateTime, ZoneId}
import java.util.Date

import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO
import org.apache.nifi.web.api.entity.ProvenanceEntity
import org.dcs.api.service.{Provenance, ProvenanceApiService}
import org.dcs.commons.error.{ErrorConstants, RESTException}
import org.dcs.commons.serde.JsonSerializerImplicits._
import org.dcs.commons.ws.JerseyRestClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

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

trait NifiProvenanceClient extends ProvenanceApiService with JerseyRestClient {
  import NifiProvenanceClient._

  override def provenance(processorId: String,
                          maxResults: Int,
                          startDate: Date = defaultStart,
                          endDate: Date = defaultEnd): Future[List[Provenance]] = {

    for {
      prequest <- submitProvenanceQuery(processorId, maxResults, startDate, endDate)
      pqresult <- provenanceQueryResult(prequest)
      presult <- provenanceResult(pqresult)
    } yield presult


  }

  // ------ Helper Methods ------


  def submitProvenanceQuery(processorId: String,
                            maxResults: Int,
                            startDate: Date,
                            endDate: Date): Future[ProvenanceEntity] =
    postAsJson(path = ProvenancePath,
      body = ProcessorProvenanceSearchRequest(processorId,
        maxResults,
        startDate,
        endDate))
      .map { response =>
        response.toObject[ProvenanceEntity]
      }

  def provenanceQueryResult(provenanceEntity: ProvenanceEntity): Future[ProvenanceEntity] =
    provenanceQueryRepeat(provenanceEntity, ProvenanceQueryMaxTries)
      .map { response =>
        try {
          if (!response.getProvenance.isFinished)
            throw new RESTException(ErrorConstants.DCS301)
          else
            response
        } finally {
          deleteAsJson(path = ProvenancePath + "/" + provenanceEntity.getProvenance.getId)
          logger.warn("Executing DELETE on " + ProvenancePath + "/" + provenanceEntity.getProvenance.getId)
        }
      }

  def provenanceQueryRepeat(provenanceEntity: ProvenanceEntity, triesLeft: Int): Future[ProvenanceEntity] = triesLeft match {
    case 0 => Future.successful(provenanceEntity)
    case _ => provenanceQuery(provenanceEntity.getProvenance.getId, provenanceEntity.getProvenance.getRequest.getClusterNodeId)
      .flatMap { response =>
        response.getProvenance.isFinished match {
          case java.lang.Boolean.TRUE => Future.successful(response)
          case java.lang.Boolean.FALSE => provenanceQuery(response.getProvenance.getId, response.getProvenance.getRequest.getClusterNodeId)
            .flatMap { nextProvenanceEntity =>
              provenanceQueryRepeat(nextProvenanceEntity, triesLeft - 1)
            }
        }
      }
  }

  def provenanceQuery(provenanceEntityId: String, clusterNodeId: String): Future[ProvenanceEntity] =
    getAsJson(path = ProvenancePath + "/" + provenanceEntityId, queryParams = params(clusterNodeId))
      .map { response =>
        response.toObject[ProvenanceEntity]
      }

  def params(clusterNodeId: String): List[(String, String)] = {
    if(clusterNodeId == null)
      List()
    else
      List(("clusterNodeId", clusterNodeId))
  }

  def provenanceResult(presult: ProvenanceEntity): Future[List[Provenance]] =
    Future.sequence(presult.getProvenance.getResults.getProvenanceEvents.asScala
      .map { pevent =>
        provenanceContent(pevent, presult)
      }.toList)

  def provenanceContent(provenanceEvent: ProvenanceEventDTO, provenanceResult: ProvenanceEntity): Future[Provenance] =
    getAsJson(path = provenanceOutput(provenanceEvent.getEventId.toString),
      queryParams = params(provenanceEvent.getClusterNodeId))
      .map { response =>
        Provenance(provenanceEvent.getId, provenanceResult.getProvenance.getId, provenanceEvent.getClusterNodeId, response)
      }


}
