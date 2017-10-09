package org.dcs.flow.nifi

import java.util

import org.apache.nifi.web.api.dto.PropertyDescriptorDTO.AllowableValueDTO
import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.entity._
import org.dcs.api.processor._
import org.dcs.api.service.{Connectable, Connection, ConnectionConfig, DropRequest, FlowComponent, FlowInstance, FlowTemplate, IOPort, ProcessorConfig, ProcessorInstance, ProcessorType}
import org.dcs.api.util.{NameId, WithArgs}
import org.dcs.flow.nifi.internal.ProcessGroup

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._



/**
  * Created by cmathew on 30/05/16.
  */


object FlowTemplate {
  def apply(template: TemplateDTO): FlowTemplate = {
    val flowTemplate = new FlowTemplate
    flowTemplate.setId(template.getId)
    flowTemplate.setUri(template.getUri)
    flowTemplate.setDescription(template.getDescription)
    flowTemplate.setTimestamp(template.getTimestamp)

    val tNameWithArgs = WithArgs(template.getName)
    flowTemplate.setName(tNameWithArgs.target)
    if(tNameWithArgs.exists(ExternalProcessorProperties.HasExternal))
      flowTemplate.setHasExternal(true)

    flowTemplate
  }
}

object ProcessGroup {
  def apply(processGroupEntity: ProcessGroupEntity): ProcessGroup = {
    val pg = new ProcessGroup

    pg.setVersion(processGroupEntity.getRevision.getVersion)
    pg.setId(processGroupEntity.getComponent.getId)
    pg.setName(processGroupEntity.getComponent.getName)
    pg
  }

}

object FlowInstance {
  val RootProcessGroupId = "root"

  def apply(processGroupEntity: ProcessGroupEntity): FlowInstance = {

    val f = new org.dcs.api.service.FlowInstance
    val contents = processGroupEntity.getComponent.getContents

    val nameId = NameId.extractFromName(processGroupEntity.getComponent.getName)


    f.setId(processGroupEntity.getComponent.getId)
    f.setVersion(processGroupEntity.getRevision.getVersion)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    if(contents != null) {
      f.setProcessors(contents.getProcessors.map(p => ProcessorInstanceAdapter(p)).toList)
      f.setConnections(contents.getConnections.map(c => ConnectionAdapter(c, processGroupEntity.getRevision.getVersion)).toList)
    }

    f
  }


  def apply(processGroupFlowEntity: ProcessGroupFlowEntity, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val flow = processGroupFlowEntity.getProcessGroupFlow.getFlow
    val bc = processGroupFlowEntity.getProcessGroupFlow.getBreadcrumb.getBreadcrumb


    val nameId = NameId.extractFromName(bc.getName)
    f.setId(processGroupFlowEntity.getProcessGroupFlow.getId)
    f.setVersion(version)
    f.setName(nameId._1)
    f.setNameId(nameId._2)

    if(flow.getProcessors.exists(p => p.getComponent.getState != NifiProcessorClient.StateRunning))
      f.setState(NifiProcessorClient.StateStopped)
    else
      f.setState(NifiProcessorClient.StateRunning)

    f.setProcessors(flow.getProcessors.map(p => ProcessorInstanceAdapter(p)).toList)
    f.setConnections(flow.getConnections.map(c => ConnectionAdapter(c.getComponent, c.getRevision.getVersion)).toList)


    f
  }



  def apply(flowSnippetEntity: FlowSnippetEntity, id: String, name: String, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val contents = flowSnippetEntity.getContents

    val nameId = NameId.extractFromName(name)
    f.setId(id)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    f.setProcessors(contents.getProcessors.map(p => ProcessorInstanceAdapter(p)).toList)
    f.setConnections(contents.getConnections.map(c => ConnectionAdapter(c, Revision.DefaultVersion)).toList)

    f
  }


  def apply(flowEntity: FlowEntity, id: String, name: String, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val flow = flowEntity.getFlow

    val nameId = NameId.extractFromName(name)

    f.setId(id)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    f.setProcessors(flow.getProcessors.map(p => ProcessorInstanceAdapter(p)).toList)
    f.setConnections(flow.getConnections.map(c => ConnectionAdapter(c.getComponent, Revision.DefaultVersion)).toList)
    f
  }

  def apply(id: String, name: String, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val nameId = NameId.extractFromName(name)
    f.setId(id)
    f.setVersion(version)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    f.setProcessors(Nil)
    f.setConnections(Nil)
    f
  }


  def apply(processGroupDTO: ProcessGroupDTO): FlowInstance  = {
    val f = new FlowInstance
    val snippet = processGroupDTO.getContents

    val nameId = NameId.extractFromName(processGroupDTO.getName)

    f.setId(processGroupDTO.getId)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setProcessors(snippet.getProcessors.map(p => ProcessorInstanceAdapter(p)).toList)
    f.setConnections(snippet.getConnections.map(c => ConnectionAdapter(c, Revision.DefaultVersion)).toList)
    f
  }
}

object FlowInstanceWithExternalConnections {

  def updateForExternalProcessors(flowInstance: FlowInstance, externalConnections: List[Connection]): FlowInstance = {

    val eps = flowInstance.processors.filter(_.isExternal)

    eps.foreach { p =>
      p.properties.get(ExternalProcessorProperties.RootInputConnectionIdKey)
        .foreach { rcId =>
          val rootConnection = externalConnections.find(_.id == rcId)
          val flowConnection = rootConnection
            .flatMap(rc => flowInstance.connections.find(_.config.source.id == rc.config.destination.id))
          flowConnection.foreach(fc => flowInstance.setConnections(flowInstance.connections.filter(_.id != fc.id)))

          flowConnection.map { fc =>
            rootConnection.foreach(rc => fc.setRelatedConnections(Set(rc)))
            if(p.processorType == RemoteProcessor.ExternalProcessorType) {
              val connectionConfig = ConnectionConfig(
                flowInstance.id,
                Connectable(p.id, FlowComponent.ExternalProcessorType, flowInstance.id),
                fc.config.destination,
                fc.config.selectedRelationships,
                fc.config.availableRelationships
              )
              Connection(connectionConfig.genId(), connectionConfig.genId(), fc.version, connectionConfig, "", "", -1, List(), Set(fc))
            } else if(p.processorType == RemoteProcessor.InputPortIngestionType) {
              fc.config.destination.setComponentType(FlowComponent.InputPortIngestionType)
              fc
            } else
              fc
          }.foreach(c => flowInstance.setConnections(c :: flowInstance.connections))
        }

      p.properties.get(ExternalProcessorProperties.RootOutputConnectionIdKey)
        .foreach { rcId =>
          val rootConnection = externalConnections.find(_.id == rcId)
          val flowConnection = rootConnection
            .flatMap(rc => flowInstance.connections.find(_.config.destination.id == rc.config.source.id))
          flowConnection.foreach(fc => flowInstance.setConnections(flowInstance.connections.filter(_.id != fc.id)))

          flowConnection.map { fc =>
            rootConnection.foreach(rc => fc.setRelatedConnections(Set(rc)))
            if(p.processorType == RemoteProcessor.ExternalProcessorType) {
              val connectionConfig = ConnectionConfig(
                flowInstance.id,
                fc.config.source,
                Connectable(p.id, FlowComponent.ExternalProcessorType, flowInstance.id),
                fc.config.selectedRelationships,
                fc.config.availableRelationships
              )
              Connection(connectionConfig.genId(), connectionConfig.genId(), fc.version, connectionConfig, "", "", -1, List(), Set(fc))
            } else
              fc
          }.foreach(c => flowInstance.setConnections(c :: flowInstance.connections))
        }
    }

    flowInstance
  }


  def apply(flowInstance: FlowInstance, externalConnections: List[Connection]): FlowInstance = {
    updateForExternalProcessors(flowInstance, externalConnections)
  }
}

object ProcessorInstanceAdapter {

  def toPossibleValues(allowableValues: util.List[AllowableValueDTO]): util.Set[PossibleValue] =
    if(allowableValues == null || allowableValues.isEmpty)
      null
    else
      allowableValues.asScala.to[Set].map(av => PossibleValue(av.getValue, av.getDisplayName, av.getDescription)).asJava


  def toRemoteProperty(processorDescriptorDTO: PropertyDescriptorDTO): RemoteProperty = {
    val remoteProperty = new RemoteProperty()
    remoteProperty.setName(processorDescriptorDTO.getName)
    remoteProperty.setDisplayName(processorDescriptorDTO.getDisplayName)
    remoteProperty.setDescription(processorDescriptorDTO.getDescription)
    remoteProperty.setDefaultValue(processorDescriptorDTO.getDefaultValue)
    remoteProperty.setPossibleValues(toPossibleValues(processorDescriptorDTO.getAllowableValues))
    remoteProperty.setRequired(processorDescriptorDTO.isRequired)
    remoteProperty.setSensitive(processorDescriptorDTO.isSensitive)
    remoteProperty.setDynamic(processorDescriptorDTO.isDynamic)
    remoteProperty
  }

  def config(processorConfigDTO: ProcessorConfigDTO): ProcessorConfig =
    ProcessorConfig(processorConfigDTO.getBulletinLevel,
      processorConfigDTO.getComments,
      processorConfigDTO.getConcurrentlySchedulableTaskCount,
      processorConfigDTO.getPenaltyDuration,
      processorConfigDTO.getSchedulingPeriod,
      processorConfigDTO.getSchedulingStrategy,
      processorConfigDTO.getYieldDuration)


  def apply(processorDTO: ProcessorDTO): ProcessorInstance = {
    val processorInstance = new ProcessorInstance

    processorInstance.setId(processorDTO.getId)
    processorInstance.setName(processorDTO.getName)
    processorInstance.setType(processorDTO.getType)
    processorInstance.setStatus({
      val state = processorDTO.getState
      if(state == null) "STANDBY" else state
    })
    processorInstance.setProcessorType(getProcessorType(processorDTO.getConfig))
    processorInstance.setRelationships(processorDTO.getRelationships.asScala.map(RemoteRelationship(_)).toSet)

    processorInstance.setProperties(valuesOrDefaults(processorDTO.getConfig))
    processorInstance.setPropertyDefinitions(Option(processorDTO.getConfig.getDescriptors).map(_.asScala.map(pd => toRemoteProperty(pd._2)).toList).getOrElse(Nil))

    ProcessorValidation
      .validate(processorInstance.id,
        processorInstance.properties,
        processorInstance.propertyDefinitions)
      .foreach(ver => processorInstance.setValidationErrors(ver))

    processorInstance.setConfig(config(processorDTO.getConfig))
    processorInstance
  }

  def apply(processorEntity: ProcessorEntity): ProcessorInstance = {
    val processorInstance = apply(processorEntity.getComponent)
    processorInstance.setVersion(processorEntity.getRevision.getVersion)
    processorInstance
  }

  def apply(processorId: String): ProcessorInstance = {
    val processorInstance = new ProcessorInstance
    processorInstance.setId(processorId)
    processorInstance.setProcessorType(RemoteProcessor.WorkerProcessorType)
    processorInstance
  }

  def getProcessorType(config: ProcessorConfigDTO): String = {
    val ptype = config.getDescriptors.get(CoreProperties.ProcessorTypeKey)
    if(ptype == null)
      RemoteProcessor.WorkerProcessorType
    else
      ptype.getDefaultValue
  }

  def valuesOrDefaults(config: ProcessorConfigDTO): Map[String, String] = {
    def default(key: String): String = {
      config.getDescriptors.asScala.toMap.get(key).map(pd => pd.getDefaultValue).getOrElse("")
    }

    config.getProperties.asScala.toMap.map(p => (p._1, if(p._2 == null || p._2.isEmpty) default(p._1) else p._2))

  }
}

object ProcessorType {
  def apply(documentedTypeDTO: DocumentedTypeDTO): ProcessorType = {
    val processorType = new ProcessorType
    processorType.setPType(documentedTypeDTO.getType)
    processorType.setDescription(documentedTypeDTO.getDescription)
    processorType.setTags(documentedTypeDTO.getTags.asScala.toList)
    processorType
  }
}

object ConnectionAdapter {

  def inputOutput(externalConnections: List[Connection]): (List[Connection], List[Connection]) = {
    var inputConnections: List[Connection] = Nil
    var outputConnections: List[Connection] = Nil

    externalConnections.foreach { ec =>
      (ec.config.source.componentType, ec.config.destination.componentType) match {
        case (FlowComponent.InputPortType, FlowComponent.InputPortType) => inputConnections = ec :: inputConnections
        case (FlowComponent.OutputPortType, FlowComponent.OutputPortType) => outputConnections = ec :: outputConnections
        case _ => // do nothing
      }
    }
    (inputConnections, outputConnections)
  }

  private def toConnectable(connectableDTO: ConnectableDTO): Connectable = {
    Connectable(connectableDTO.getId,
      connectableDTO.getType,
      connectableDTO.getGroupId,
      Map(),
      connectableDTO.getName)
  }

  private def toConnectable(connectableId: String, connectableType: String, groupId: String): Connectable = {
    Connectable(connectableId, connectableType, groupId)
  }

  def apply(connectionDTO: ConnectionDTO, version: Long): Connection = {

    new Connection(connectionDTO.getId,
      connectionDTO.getName,
      version,
      ConnectionConfig(connectionDTO.getParentGroupId,
        toConnectable(connectionDTO.getSource),
        toConnectable(connectionDTO.getDestination),
        Option(connectionDTO.getSelectedRelationships).map(_.asScala.toSet).getOrElse(Set()),
        Option(connectionDTO.getAvailableRelationships).map(_.asScala.toSet).getOrElse(Set())),
      connectionDTO.getFlowFileExpiration,
      connectionDTO.getBackPressureDataSizeThreshold,
      connectionDTO.getBackPressureObjectThreshold,
      connectionDTO.getPrioritizers.asScala.toList,
      Set())
  }


  def apply(connectionId: String,
            sourceId: String,
            sourceType: String,
            sourceGroupId: String,
            destinationId: String,
            destinationType: String,
            destinationGroupId: String): Connection = {

    new Connection(connectionId,
      "",
      Revision.DefaultVersion,
      ConnectionConfig("",
        toConnectable(sourceId, sourceType, sourceGroupId),
        toConnectable(destinationId, destinationType, destinationGroupId),
        Set(),
        Set()),
      "",
      "",
      -1,
      List(),
      Set())
  }

  def apply(connectionEntity: ConnectionEntity): Connection = {
    if(connectionEntity.getComponent != null && connectionEntity.getRevision != null)
      ConnectionAdapter(connectionEntity.getComponent, connectionEntity.getRevision.getVersion)
    else
      ConnectionAdapter(connectionEntity.getId,
        connectionEntity.getSourceId,
        connectionEntity.getSourceType,
        connectionEntity.getSourceGroupId,
        connectionEntity.getDestinationId,
        connectionEntity.getDestinationType,
        connectionEntity.getDestinationGroupId)
  }

}

object RemoteRelationship {
  def apply(relationship: RelationshipDTO): RemoteRelationship = {
    new RemoteRelationship(relationship.getName, relationship.getDescription, relationship.isAutoTerminate)
  }
}

object IOPortAdapter {
  def apply(portEntity: PortEntity): IOPort = {
    val componentO = Option(portEntity.getComponent)
    val revisionO = Option(portEntity.getRevision)
    new IOPort(portEntity.getId,
      componentO.map(_.getName).orNull,
      revisionO.map(_.getVersion.toLong).getOrElse(Revision.DefaultVersion),
      portEntity.getPortType,
      componentO.map(_.getState).orNull)
  }
}

object DropRequestAdapter {
  def apply(dropRequestEntity: DropRequestEntity): DropRequest = {
    val dropRequestDTO = dropRequestEntity.getDropRequest
    DropRequest(dropRequestDTO.getId, dropRequestDTO.isFinished, dropRequestDTO.getCurrentCount)
  }
}