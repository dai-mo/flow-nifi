package org.dcs.flow.nifi

import org.apache.nifi.web.api.dto._
import org.apache.nifi.web.api.entity.{FlowSnippetEntity, ProcessGroupEntity, SnippetEntity}
import org.dcs.api.service._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/05/16.
  */

object FlowTemplate {
  def apply(template: TemplateDTO): FlowTemplate = {
    val flowTemplate = new FlowTemplate
    flowTemplate.setId(template.getId)
    flowTemplate
  }
}

object FlowInstance {
  def apply(processGroupEntity: ProcessGroupEntity): FlowInstance = {

    val f = new FlowInstance
    val contents = processGroupEntity.getProcessGroup.getContents

    f.setVersion(processGroupEntity.getRevision.getVersion.toString)
    f.setId(processGroupEntity.getProcessGroup.getParentGroupId)
    f.setProcessors(contents.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(contents.getConnections.map(c => Connection(c)).toList)
    f
  }

  def apply(flowSnippetEntity: FlowSnippetEntity): FlowInstance  = {
    val f = new FlowInstance
    val contents = flowSnippetEntity.getContents

    f.setVersion(flowSnippetEntity.getRevision.getVersion.toString)
    f.setProcessors(contents.getProcessors.map(p => ProcessorInstance(p)).toList)
    f.setConnections(contents.getConnections.map(c => Connection(c)).toList)
    f
  }

  def apply(snippetEntity: SnippetEntity): FlowInstance  = {
    val f = new FlowInstance
    val snippet = snippetEntity.getSnippet
    f.setVersion(snippetEntity.getRevision.getVersion.toString)
    f.setId(snippet.getId)
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
    val c = new Connection
    c.setId(connection.getId)
    c
  }

  def apply(connectionId: String): Connection = {
    val c = new Connection
    c.setId(connectionId)
    c
  }
}
