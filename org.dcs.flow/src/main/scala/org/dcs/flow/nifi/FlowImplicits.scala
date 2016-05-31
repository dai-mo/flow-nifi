package org.dcs.flow.nifi

import org.apache.nifi.web.api.dto.{ConnectionDTO, ProcessorDTO}
import org.apache.nifi.web.api.entity.FlowSnippetEntity
import org.dcs.flow.model.{Flow, Processor, Connection}

import scala.collection.JavaConversions._

/**
  * Created by cmathew on 30/05/16.
  */

object NifiFlowImplicits {

  implicit class FlowConv(flowSnippet: FlowSnippetEntity) {
    def toFlow: Flow = {
      val f = new Flow
      val contents = flowSnippet.getContents
      f.processors = contents.getProcessors.map(p => p.toProcessor).toList
      f.connections = contents.getConnections.map(p => p.toConnection).toList
      f
    }
  }

  implicit class ProcessorConv(processor: ProcessorDTO)  {
    def toProcessor: Processor = {
      val p = new Processor
      p.id = processor.getId
      p
    }
  }

  implicit class ConnectionConv(connection: ConnectionDTO)  {
    def toConnection: Connection = {
      val c = new Connection
      c.id = connection.getId
      c
    }
  }
}

class NifiFlowImplicits {}
