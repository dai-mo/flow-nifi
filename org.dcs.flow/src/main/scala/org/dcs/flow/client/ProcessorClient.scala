package org.dcs.flow.client

import scala.collection.JavaConverters._
import collection.JavaConversions._
import org.dcs.api.model.Processor
import org.dcs.flow.ProcessorApi


trait ProcessorClient  {
    
  this: ProcessorApi => 
  
  def typesSearchTags(str:String): List[Processor] = {
    types.filter( dtype => dtype.getTags.exists(tag => tag.contains(str))) 
  }
}