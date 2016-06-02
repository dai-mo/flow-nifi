package org.dcs.flow.client


import org.dcs.flow.ProcessorClient
import org.dcs.flow.model.ProcessorType


trait ProcessorApi  {
    
  this: ProcessorClient =>

  def typesSearchTags(str:String, clientToken: String): List[ProcessorType] = {
    types(clientToken).filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
  }
}