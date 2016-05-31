package org.dcs.flow.nifi

import org.dcs.api.model.Processor
import org.dcs.commons.JsonSerializerImplicits._

import scala.collection.JavaConverters._
import org.apache.nifi.web.api.entity.ProcessorTypesEntity
import java.util.ArrayList

import org.dcs.commons.config.ConfigurationFacade
import javax.ws.rs.core.Response
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.core.MediaType

import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.dcs.flow.ProcessorClient
import org.dcs.flow.BaseRestClient
import org.dcs.flow.model.ProcessorType

object NifiProcessorClient  {
  
  val TypesPath = "/controller/processor-types"

}

trait NifiProcessorClient extends ProcessorClient with NifiBaseRestClient {
  
  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProcessorClient])
  
  import NifiProcessorClient._

  override def types(): List[ProcessorType] = {
    val processorTypes = getAsJson(TypesPath).toObject[ProcessorTypesEntity]
    processorTypes.getProcessorTypes.asScala.map(dt => ProcessorType(dt)).toList
  }

  override def start(processorId: String): Processor = {
    new Processor
  }
}