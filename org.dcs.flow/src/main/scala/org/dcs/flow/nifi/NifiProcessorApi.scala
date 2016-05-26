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
import org.dcs.flow.ProcessorApi

object NifiProcessorApi {
  val BaseUrl = ConfigurationFacade.config.nifiBaseUrl  
  val TypesPath = "/controller/processor-types"
}

trait NifiProcessorApi extends ProcessorApi {
  val logger: Logger = LoggerFactory.getLogger(classOf[NifiProcessorApi])
  
  import NifiProcessorApi._
  
  override def response(path: String): Response = {
    ClientBuilder.newClient.target(BaseUrl)
      .path(path)            
      .request(MediaType.APPLICATION_JSON)
      .get
  }
    
  override def types(): List[Processor] = {
    
    val processorTypes = response(TypesPath).readEntity(classOf[String]).toObject[ProcessorTypesEntity]
    processorTypes.getProcessorTypes.asScala.map(dt => {
      val p = new Processor()
      p.setPtype(dt.getType)
      p.setDescription(dt.getDescription)
      p.setTags(new ArrayList(dt.getTags))
      p
    }).toList
  }
}