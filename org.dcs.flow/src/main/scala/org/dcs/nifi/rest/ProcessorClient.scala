package org.dcs.nifi.rest

import scala.collection.JavaConverters._
import collection.JavaConversions._
import org.apache.nifi.web.api.dto.DocumentedTypeDTO
import org.apache.nifi.web.api.entity.ProcessorTypesEntity
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import org.dcs.commons.config.ConfigurationFacade
import org.dcs.api.model.Processor


trait ProcessorClient  {
    
  this: ProcessorApi => 
  
  def typesSearchTags(str:String): List[Processor] = {
    types.filter( dtype => dtype.getTags.exists(tag => tag.contains(str))) 
  }
}