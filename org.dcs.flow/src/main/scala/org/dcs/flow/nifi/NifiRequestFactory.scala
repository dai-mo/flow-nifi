package org.dcs.flow.nifi

import org.apache.nifi.web.api.dto.{PositionDTO, ProcessGroupDTO, ProcessorDTO, RevisionDTO}
import org.apache.nifi.web.api.entity.{InstantiateTemplateRequestEntity, ProcessGroupEntity, ProcessorEntity}

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

object FlowInstanceRequest {
  def apply(templateId: String): InstantiateTemplateRequestEntity = {
    val itre = new InstantiateTemplateRequestEntity
    itre.setOriginX(Position.X)
    itre.setOriginY(Position.Y)
    itre.setTemplateId(templateId)

    itre
  }
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
