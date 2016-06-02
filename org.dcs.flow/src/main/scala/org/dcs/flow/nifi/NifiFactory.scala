package org.dcs.flow.nifi

import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.entity.{FlowSnippetEntity, ProcessGroupEntity, SnippetEntity}
import org.dcs.flow.model._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/05/16.
  */

object FlowTemplate {
  def apply(template: TemplateDTO): FlowTemplate = {
    val flowTemplate = new FlowTemplate
    flowTemplate.id = template.getId
    flowTemplate
  }
}

object FlowInstance {
  def apply(processGroupEntity: ProcessGroupEntity): FlowInstance = {

    val f = new FlowInstance
    val contents = processGroupEntity.getProcessGroup.getContents

    f.version = processGroupEntity.getRevision.getVersion.toString
    f.id = processGroupEntity.getProcessGroup.getParentGroupId
    f.processors = contents.getProcessors.map(p => ProcessorInstance(p)).toList
    f.connections = contents.getConnections.map(c => Connection(c)).toList
    f
  }

  def apply(flowSnippetEntity: FlowSnippetEntity): FlowInstance  = {
    val f = new FlowInstance
    val contents = flowSnippetEntity.getContents

    f.version = flowSnippetEntity.getRevision.getVersion.toString
    f.processors = contents.getProcessors.map(p => ProcessorInstance(p)).toList
    f.connections = contents.getConnections.map(c => Connection(c)).toList
    f
  }

  def apply(snippetEntity: SnippetEntity): FlowInstance  = {
    val f = new FlowInstance
    val snippet = snippetEntity.getSnippet
    f.version = snippetEntity.getRevision.getVersion.toString
    f.id = snippet.getId
    f.processors = snippet.getProcessors.map(p => ProcessorInstance(p)).toList
    f.connections = snippet.getConnections.map(c => Connection(c)).toList
    f
  }
}

object ProcessorInstance {

  def apply(processorDTO: ProcessorDTO): ProcessorInstance = {
    val processorInstance = new ProcessorInstance
    processorInstance.id = processorDTO.getId
    processorInstance.status = {
      val state = processorDTO.getState
      if(state == null) "STANDBY" else state
    }
    processorInstance
  }

  def apply(processorId: String): ProcessorInstance = {
    val processorInstance = new ProcessorInstance
    processorInstance.id = processorId

    processorInstance
  }
}

object ProcessorType {
  def apply(documentedTypeDTO: DocumentedTypeDTO): ProcessorType = {
    val processorType = new ProcessorType
    processorType.pType = documentedTypeDTO.getType
    processorType.description = documentedTypeDTO.getDescription
    processorType.tags = documentedTypeDTO.getTags.asScala.toList
    processorType
  }
}

object Connection {
  def apply(connection: ConnectionDTO): Connection = {
    val c = new Connection
    c.id = connection.getId
    c
  }

  def apply(connectionId: String): Connection = {
    val c = new Connection
    c.id = connectionId
    c
  }
}
