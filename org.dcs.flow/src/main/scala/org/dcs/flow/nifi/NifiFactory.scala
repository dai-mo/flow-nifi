package org.dcs.flow.nifi

import org.apache.nifi.web.api.dto.{ConnectionDTO, DocumentedTypeDTO, ProcessorDTO, TemplateDTO}
import org.apache.nifi.web.api.entity.FlowSnippetEntity
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
  def apply(flowSnippet: FlowSnippetEntity): FlowInstance = {
    val f = new FlowInstance
    val contents = flowSnippet.getContents
    f.processors = contents.getProcessors.map(p => ProcessorInstance(p)).toList
    f.connections = contents.getConnections.map(c => Connection(c)).toList
    f
  }
}

object ProcessorInstance {

  def apply(processorDTO: ProcessorDTO): ProcessorInstance = {
    val processorInstance = new ProcessorInstance
    processorInstance.id = processorDTO.getId
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
}
