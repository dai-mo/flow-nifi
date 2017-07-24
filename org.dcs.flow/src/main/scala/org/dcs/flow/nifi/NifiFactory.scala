package org.dcs.flow.nifi

import java.util

import org.apache.nifi.web.api.dto.PropertyDescriptorDTO.AllowableValueDTO
import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.entity._
import org.dcs.api.processor._
import org.dcs.api.service.{Connectable, Connection, ConnectionConfig, FlowInstance, FlowTemplate, ProcessorConfig, ProcessorInstance, ProcessorType}
import org.dcs.flow.nifi.internal.{ProcessGroup, ProcessGroupHelper}

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
    flowTemplate.setName(template.getName)
    flowTemplate.setDescription(template.getDescription)
    flowTemplate.setTimestamp(template.getTimestamp)
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
  def apply(processGroupEntity: ProcessGroupEntity): FlowInstance = {

    val f = new org.dcs.api.service.FlowInstance
    val contents = processGroupEntity.getComponent.getContents

    val nameId = ProcessGroupHelper.extractFromName(processGroupEntity.getComponent.getName)


    f.setId(processGroupEntity.getComponent.getId)
    f.setVersion(processGroupEntity.getRevision.getVersion)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    if(contents != null) {
      f.setProcessors(contents.getProcessors.map(p => ProcessorInstance(p)).toList)
      f.setConnections(contents.getConnections.map(c => Connection(c, processGroupEntity.getRevision.getVersion)).toList)
    }
    f
  }


  def apply(processGroupFlowEntity: ProcessGroupFlowEntity, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val flow = processGroupFlowEntity.getProcessGroupFlow.getFlow
    val bc = processGroupFlowEntity.getProcessGroupFlow.getBreadcrumb.getBreadcrumb


    val nameId = ProcessGroupHelper.extractFromName(bc.getName)
    f.setId(processGroupFlowEntity.getProcessGroupFlow.getId)
    f.setVersion(version)
    f.setName(nameId._1)
    f.setNameId(nameId._2)

    if(flow.getProcessors.exists(p => p.getComponent.getState != NifiProcessorClient.StateRunning))
      f.setState(NifiProcessorClient.StateStopped)
    else
      f.setState(NifiProcessorClient.StateRunning)

    f.setProcessors(flow.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(flow.getConnections.map(c => Connection(c.getComponent, Revision.DefaultVersion)).toList)
    f
  }

  def apply(flowSnippetEntity: FlowSnippetEntity, id: String, name: String, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val contents = flowSnippetEntity.getContents

    val nameId = ProcessGroupHelper.extractFromName(name)
    f.setId(id)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    f.setProcessors(contents.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(contents.getConnections.map(c => Connection(c, Revision.DefaultVersion)).toList)

    f
  }

  def apply(flowEntity: FlowEntity, id: String, name: String, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val flow = flowEntity.getFlow


    val nameId = ProcessGroupHelper.extractFromName(name)

    f.setId(id)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setState(NifiProcessorClient.StateNotStarted)
    f.setProcessors(flow.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(flow.getConnections.map(c => Connection(c.getComponent, Revision.DefaultVersion)).toList)
    f
  }

  def apply(id: String, name: String, version: Long): FlowInstance  = {
    val f = new FlowInstance
    val nameId = ProcessGroupHelper.extractFromName(name)
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

    val nameId = ProcessGroupHelper.extractFromName(processGroupDTO.getName)

    f.setId(processGroupDTO.getId)
    f.setName(nameId._1)
    f.setNameId(nameId._2)
    f.setProcessors(snippet.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(snippet.getConnections.map(c => Connection(c, Revision.DefaultVersion)).toList)
    f
  }
}



object ProcessorInstance {

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

object Connection {
  private def toConnectable(connectableDTO: ConnectableDTO): Connectable = {
    Connectable(connectableDTO.getId,
      connectableDTO.getType,
      connectableDTO.getGroupId)
  }

  def apply(connectionDTO: ConnectionDTO, version: Long): Connection = {
    new Connection(connectionDTO.getId,
      connectionDTO.getName,
      version,
      ConnectionConfig(connectionDTO.getParentGroupId,
        toConnectable(connectionDTO.getSource),
        toConnectable(connectionDTO.getDestination),
        connectionDTO.getSelectedRelationships.asScala.toSet,
        connectionDTO.getAvailableRelationships.asScala.toSet),
      connectionDTO.getFlowFileExpiration,
      connectionDTO.getBackPressureDataSizeThreshold,
      connectionDTO.getBackPressureObjectThreshold,
      connectionDTO.getPrioritizers.asScala.toList)
  }

  def apply(connectionEntity: ConnectionEntity): Connection = {
    Connection(connectionEntity.getComponent, connectionEntity.getRevision.getVersion)
  }
}

object RemoteRelationship {
  def apply(relationship: RelationshipDTO): RemoteRelationship = {
    new RemoteRelationship(relationship.getName, relationship.getDescription, relationship.isAutoTerminate)
  }
}
