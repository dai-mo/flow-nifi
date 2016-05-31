package org.dcs.flow.client


import org.dcs.flow.ProcessorClient
import org.dcs.flow.model.ProcessorType


trait ProcessorApi  {
    
  this: ProcessorClient =>

  def typesSearchTags(str:String): List[ProcessorType] = {
    types.filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
  }
}