package org.dcs.flow.nifi

import java.time.{LocalDateTime, ZoneId}
import java.util.Date

import org.apache.avro.Schema
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO
import org.apache.nifi.web.api.entity.ProvenanceEntity
import org.dcs.api.processor.CoreProperties
import org.dcs.api.service.{Provenance, ProvenanceApiService}
import org.dcs.commons.error.{ErrorConstants, HttpException}
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore
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
  val ProvenanceQueryMaxTries = 50
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
                          processorType: String,
                          maxResults: Int,
                          startDate: Date = defaultStart,
                          endDate: Date = defaultEnd): Future[List[Provenance]] = {

    for {
      prequest <- submitProvenanceQuery(processorId, maxResults, startDate, endDate)
      pqresult <- provenanceQueryResult(prequest)
      presult <- provenanceResult(pqresult, processorType)
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
    if(provenanceEntity.getProvenance.isFinished)
      Future(provenanceEntity)
    else
      provenanceQueryRepeat(provenanceEntity, ProvenanceQueryMaxTries)
        .map { response =>
          try {
            if (!response.getProvenance.isFinished)
              throw new HttpException(ErrorConstants.DCS301.http(400))
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

  def provenanceResult(pResult: ProvenanceEntity, pType: String): Future[List[Provenance]] =
    Future.sequence(pResult.getProvenance.getResults.getProvenanceEvents.asScala
      .filter { pevent =>
        val schema = pevent.getAttributes.asScala.find(_.getName == CoreProperties.WriteSchemaIdKey).map(_.getValue).flatMap(AvroSchemaStore.get)
        schema.isDefined
      }
      .map { pevent => {
        val schema = pevent.getAttributes.asScala.find(_.getName == CoreProperties.WriteSchemaIdKey).map(_.getValue).flatMap(AvroSchemaStore.get)
        provenanceContent(pevent, pResult, pType, schema)
      }}.toList)

  def provenanceContent(provenanceEvent: ProvenanceEventDTO,
                        provenanceResult: ProvenanceEntity,
                        processorType: String,
                        schema: Option[Schema]): Future[Provenance] = {

    get(path = provenanceOutput(provenanceEvent.getEventId.toString),
      queryParams = params(provenanceEvent.getClusterNodeId))
      .map { response => {
        val content = response.readEntity(classOf[Array[Byte]])
        Provenance(provenanceEvent.getId,
          provenanceResult.getProvenance.getId,
          provenanceEvent.getClusterNodeId,
          Array[Byte](),
          content.deSerToJsonString(schema, schema),
          provenanceEvent.getEventTime,
          provenanceEvent.getRelationship)
      }
      }
  }

}
