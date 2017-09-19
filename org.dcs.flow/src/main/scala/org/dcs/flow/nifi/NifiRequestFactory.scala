package org.dcs.flow.nifi

import java.util.{Date, UUID}

import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.dto.provenance.{ProvenanceDTO, ProvenanceRequestDTO}
import org.apache.nifi.web.api.entity._
import org.dcs.api.processor.RemoteProcessor
import org.dcs.api.service.{Connectable, Connection, ConnectionConfig, ProcessorInstance, ProcessorServiceDefinition}

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

  val DefaultVersion: Long = -1

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
      case RemoteProcessor.ExternalProcessorType =>
        "ExternalProcessor"
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

object FlowConnectionRequest {
  def apply(sourceConnectable: Connectable,
            destinationConnectable: Connectable,
            selectedRelationships: Set[String],
            availableRelationships: Set[String],
            id: Option[String],
            flowInstanceId: String,
            name: Option[String],
            flowFileExpiration: Option[String],
            backPressureDataSize: Option[String],
            backPressureObjectThreshold: Option[Long],
            prioritizers: Option[List[String]],
            clientId: String,
            version: Long): ConnectionEntity = {
    val connectionEntity = new ConnectionEntity
    connectionEntity.setSourceId(sourceConnectable.id)
    connectionEntity.setSourceGroupId(sourceConnectable.flowInstanceId)
    connectionEntity.setSourceType(sourceConnectable.componentType)

    connectionEntity.setDestinationId(destinationConnectable.id)
    connectionEntity.setDestinationGroupId(destinationConnectable.flowInstanceId)
    connectionEntity.setDestinationType(destinationConnectable.componentType)

    val connectionDTO = new ConnectionDTO
    id.foreach(connectionDTO.setId)
    connectionDTO.setParentGroupId(flowInstanceId)

    val sourceConnectableDTO = new ConnectableDTO
    sourceConnectableDTO.setId(sourceConnectable.id)
    sourceConnectableDTO.setGroupId(sourceConnectable.flowInstanceId)
    sourceConnectableDTO.setType(sourceConnectable.componentType)
    connectionDTO.setSource(sourceConnectableDTO)

    val destinationConnectableDTO = new ConnectableDTO
    destinationConnectableDTO.setId(destinationConnectable.id)
    destinationConnectableDTO.setGroupId(destinationConnectable.flowInstanceId)
    destinationConnectableDTO.setType(destinationConnectable.componentType)
    connectionDTO.setDestination(destinationConnectableDTO)

    connectionDTO.setSelectedRelationships(selectedRelationships.asJava)
    connectionDTO.setAvailableRelationships(availableRelationships.asJava)
    name.foreach(connectionDTO.setName)
    flowFileExpiration.foreach(connectionDTO.setFlowFileExpiration)
    backPressureDataSize.foreach(connectionDTO.setBackPressureDataSizeThreshold)
    backPressureObjectThreshold.foreach(bpot => connectionDTO.setBackPressureObjectThreshold(bpot))

    connectionDTO.setPrioritizers(prioritizers.getOrElse(Nil).asJava)
    connectionEntity.setComponent(connectionDTO)

    connectionEntity.setRevision(Revision(version, clientId))

    connectionEntity
  }

  def apply(connection: Connection, clientId: String): ConnectionEntity = {
    apply(connection.config.source,
      connection.config.destination,
      connection.config.selectedRelationships,
      connection.config.availableRelationships,
      Option(connection.id),
      connection.config.flowInstanceId,
      Option(connection.name),
      Option(connection.flowFileExpiration),
      Option(connection.backPressureDataSize),
      Option(connection.backPressureObjectThreshold),
      Option(connection.prioritizers),
      clientId,
      connection.version)
  }

  def apply(connectionConfig: ConnectionConfig, clientId: String): ConnectionEntity = {
    apply(connectionConfig.source,
      connectionConfig.destination,
      connectionConfig.selectedRelationships,
      Set(),
      None,
      connectionConfig.flowInstanceId,
      None,
      None,
      None,
      None,
      None,
      clientId,
      0)
  }
}

object FlowPortRequest {


  def apply(portType: String, portName: String, clientId: String): PortEntity = {
    val portEntity = new PortEntity
    portEntity.setRevision(Revision(0L, clientId))
    portEntity.setPortType(portType)

    val portDTO = new PortDTO
    portDTO.setName(portName)

    portEntity.setComponent(portDTO)
    portEntity
  }

  def apply(portType: String, clientId: String): PortEntity =
    apply(portType, UUID.randomUUID().toString, clientId)
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

