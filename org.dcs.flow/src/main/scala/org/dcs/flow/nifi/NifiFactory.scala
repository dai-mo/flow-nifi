package org.dcs.flow.nifi

import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.entity.{FlowSnippetEntity, ProcessGroupEntity, SnippetEntity}
import org.dcs.api.service._
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
    flowTemplate.setName(template.getName)
    flowTemplate.setDescription(template.getDescription)
    flowTemplate.setTimestamp(template.getTimestamp)
    flowTemplate
  }
}

object ProcessGroup {
  def apply(processGroupEntity: ProcessGroupEntity): ProcessGroup = {
    val pg = new ProcessGroup

    pg.setId(processGroupEntity.getProcessGroup.getId)
    pg.setName(processGroupEntity.getProcessGroup.getName)
    pg
  }

  def apply(processGroupDTO: ProcessGroupDTO): ProcessGroup = {
    val pg = new ProcessGroup

    pg.setId(processGroupDTO.getId)
    pg.setName(processGroupDTO.getName)
    pg
  }
}

object FlowInstance {
  def apply(processGroupEntity: ProcessGroupEntity): FlowInstance = {

    val f = new FlowInstance
    val contents = processGroupEntity.getProcessGroup.getContents

    f.setVersion(processGroupEntity.getRevision.getVersion.toString)
    f.setId(processGroupEntity.getProcessGroup.getParentGroupId)
    f.setName(processGroupEntity.getProcessGroup.getName)
    f.setProcessors(contents.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(contents.getConnections.map(c => Connection(c)).toList)
    f
  }

  def apply(flowSnippetEntity: FlowSnippetEntity, id: String): FlowInstance  = {
    val f = new FlowInstance
    val contents = flowSnippetEntity.getContents

    f.setId(id)
    f.setVersion(flowSnippetEntity.getRevision.getVersion.toString)
    f.setProcessors(contents.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(contents.getConnections.map(c => Connection(c)).toList)
    f
  }

  def apply(snippetEntity: SnippetEntity): FlowInstance  = {
    val f = new FlowInstance
    val snippet = snippetEntity.getSnippet

    f.setId(snippet.getId)
    f.setVersion(snippetEntity.getRevision.getVersion.toString)
    f.setProcessors(snippet.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(snippet.getConnections.map(c => Connection(c)).toList)
    f
  }

  def apply(processGroupDTO: ProcessGroupDTO): FlowInstance  = {
    val f = new FlowInstance
    val snippet = processGroupDTO.getContents

    f.setId(processGroupDTO.getId)
    f.setProcessors(snippet.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(snippet.getConnections.map(c => Connection(c)).toList)
    f
  }
}

object ProcessorInstance {

  def apply(processorDTO: ProcessorDTO): ProcessorInstance = {
    val processorInstance = new ProcessorInstance
    processorInstance.setId(processorDTO.getId)
    processorInstance.setStatus({
      val state = processorDTO.getState
      if(state == null) "STANDBY" else state
    })
    processorInstance
  }

  def apply(processorId: String): ProcessorInstance = {
    val processorInstance = new ProcessorInstance
    processorInstance.setId(processorId)

    processorInstance
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
  def apply(connection: ConnectionDTO): Connection = {
    Connection(connection.getId,
      sourceId = connection.getSource.getId,
      sourceType = connection.getSource.getType,
      destinationId = connection.getDestination.getId,
      destinationType = connection.getDestination.getType)
  }

  def apply(connectionId: String,
            sourceId: String = "",
            sourceType: String = "",
            destinationId: String = "",
            destinationType: String = ""): Connection = {
    val c = new Connection
    c.setId(connectionId)
    val source = ConnectionPort(sourceId, sourceType)
    c.setSource(source)
    val destination = ConnectionPort(destinationId, destinationType)
    c.setDestination(destination)
    c
  }
}
