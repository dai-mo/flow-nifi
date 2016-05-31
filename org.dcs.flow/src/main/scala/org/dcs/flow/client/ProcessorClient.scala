package org.dcs.flow.client


import org.dcs.flow.ProcessorApi
import org.dcs.flow.model.ProcessorType


trait ProcessorClient  {
    
  this: ProcessorApi =>

  def typesSearchTags(str:String): List[ProcessorType] = {
    types.filter( dtype => dtype.tags.exists(tag => tag.contains(str)))
  }
}