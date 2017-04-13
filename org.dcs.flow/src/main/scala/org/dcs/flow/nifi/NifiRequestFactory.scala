package org.dcs.flow.nifi

import java.util.Date

import org.apache.nifi.web.api.dto.provenance.{ProvenanceDTO, ProvenanceRequestDTO}
import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.entity.{InstantiateTemplateRequestEntity, ProcessGroupEntity, ProcessorEntity, ProvenanceEntity}
import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service.{FlowInstance, ProcessorConfig, ProcessorInstance, ProcessorServiceDefinition}
import org.dcs.flow.nifi.internal.ProcessGroupHelper

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
/**
  * Created by cmathew on 11/08/16.
  */



object Position {
  val X = 100.0
  val Y = 100.0

  def apply(): PositionDTO = {
    new PositionDTO(X, Y)
  }
}

object Revision {
  def apply(version: Long, clientId: String): RevisionDTO = {
    val rev: RevisionDTO = new RevisionDTO
    rev.setVersion(version)
    rev.setClientId(clientId)

    rev
  }

  def params(version: Long, clientId: String): List[(String, String)] =
    List(("version", version.toString), ("clientId", clientId))

  def params(clientId: String): List[(String, String)] =
    List(("clientId", clientId))
}

object FlowInstanceContainerRequest {
  def apply(name: String, clientId: String): ProcessGroupEntity = {

    val pg: ProcessGroupDTO = new ProcessGroupDTO
    pg.setName(name)
    pg.setPosition(Position())

    val pge = new ProcessGroupEntity
    pge.setRevision(Revision(0.0.toLong, clientId))
    pge.setComponent(pg)
    pge
  }
}

object FlowProcessorRequest {

  def clientProcessorType(psd: ProcessorServiceDefinition): String = {
    "org.dcs.nifi.processors." + (psd.processorType match {
      case RemoteProcessor.IngestionProcessorType =>
        if(psd.stateful) "IngestionStatefulProcessor" else "IngestionProcessor"
      case RemoteProcessor.WorkerProcessorType =>
        if(psd.stateful) "WorkerStatefulProcessor" else "WorkerProcessor"
      case RemoteProcessor.SinkProcessorType =>
        if(psd.stateful) "SinkStatefulProcessor" else "SinkProcessor"
      case _ => throw new IllegalArgumentException("Unknown Processor Type : " + psd.processorType)
    })
  }



  def apply(psd: ProcessorServiceDefinition, clientId: String): ProcessorEntity = {
    val processorEntity = new ProcessorEntity
    val processorDTO = new ProcessorDTO

    processorDTO.setName(psd.processorServiceClassName.split("\\.").last)

    processorDTO.setType(clientProcessorType(psd))

    processorEntity.setComponent(processorDTO)

    processorEntity.setRevision(Revision(0.0.toLong, clientId))
    processorEntity.setPosition(Position())

    processorEntity
  }


}

object FlowProcessorUpdateRequest {
  def config(processorInstance: ProcessorInstance): ProcessorConfigDTO = {

    val processorConfigDTO = new ProcessorConfigDTO
    processorConfigDTO.setAutoTerminatedRelationships(processorInstance.getConfig.autoTerminatedRelationships.asJava)
    processorConfigDTO.setBulletinLevel(processorInstance.getConfig.bulletinLevel)
    processorConfigDTO.setComments(processorInstance.getConfig.comments)
    processorConfigDTO.setConcurrentlySchedulableTaskCount(processorInstance.getConfig.concurrentlySchedulableTaskCount)
    processorConfigDTO.setPenaltyDuration(processorInstance.getConfig.penaltyDuration)
    processorConfigDTO.setSchedulingPeriod(processorInstance.getConfig.schedulingPeriod)
    processorConfigDTO.setSchedulingStrategy(processorInstance.getConfig.schedulingStrategy)
    processorConfigDTO.setYieldDuration(processorInstance.getConfig.yieldDuration)
    processorConfigDTO.setProperties(processorInstance.properties.asJava)

    processorConfigDTO
  }

  def apply(properties: Map[String, String], processorEntity: ProcessorEntity): ProcessorEntity = {
    processorEntity.getComponent.getConfig.setProperties(properties.asJava)
    processorEntity
  }

  def apply(autoTerminateRelationships: Set[String], processorEntity: ProcessorEntity): ProcessorEntity = {
    processorEntity.getComponent.getConfig.setAutoTerminatedRelationships(autoTerminateRelationships.asJava)
    processorEntity
  }

  def apply(processorInstance: ProcessorInstance, clientId: String): ProcessorEntity = {
    val processorEntity = new ProcessorEntity
    val processorDTO = new ProcessorDTO

    processorEntity.setRevision(Revision(processorInstance.version, clientId))

    processorDTO.setConfig(config(processorInstance))
    processorDTO.setName(processorInstance.name)
    processorDTO.setState(processorInstance.status)
    processorDTO.setId(processorInstance.id)

    processorEntity.setComponent(processorDTO)
    processorEntity
  }
}

object FlowInstanceRequest {
  def apply(templateId: String): InstantiateTemplateRequestEntity = {
    val itre = new InstantiateTemplateRequestEntity
    itre.setOriginX(Position.X)
    itre.setOriginY(Position.Y)
    itre.setTemplateId(templateId)

    itre
  }
}

// FIXME: There should be a Nifi counterpart for this but did not find it yet
case class FlowInstanceStartRequest(@BeanProperty var id: String,
                                    @BeanProperty var state: String) {
  def this() = this("", "")
}

object ProcessorStateUpdateRequest {
  def apply(processorId: String, state: String, currentVersion: Long, clientId: String): ProcessorEntity = {
    val processor = new ProcessorDTO
    processor.setId(processorId)
    processor.setState(state)

    val pe = new ProcessorEntity
    pe.setComponent(processor)
    pe.setRevision(Revision(currentVersion, clientId))

    pe
  }
}


object ProcessorProvenanceSearchRequest {
  def apply(processorId: String, maxResults: Int, startDate: Date, endDate: Date): ProvenanceEntity = {
    val request = new ProvenanceRequestDTO
    request.setMaxResults(maxResults)
    request.setStartDate(startDate)
    request.setEndDate(endDate)
    request.setSearchTerms(Map("ProcessorID" -> processorId).asJava)

    val provenance = new ProvenanceDTO
    provenance.setRequest(request)

    val provenanceEntity = new ProvenanceEntity
    provenanceEntity.setProvenance(provenance)
    provenanceEntity
  }
}

