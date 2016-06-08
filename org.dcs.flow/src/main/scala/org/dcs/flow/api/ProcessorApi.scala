package org.dcs.flow.api

import org.dcs.api.service.{ProcessorApiService, ProcessorType}


trait ProcessorApi  {
    
  this: ProcessorApiService =>

  def typesSearchTags(str:String, clientToken: String): List[ProcessorType] = {
    types(clientToken).filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
  }
}