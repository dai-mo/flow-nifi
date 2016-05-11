package org.dcs.nifi.processors

import org.apache.nifi.processor.Relationship
import org.dcs.api.service.FlowModule
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship.{ Builder => RelationshipBuilder }
import org.apache.nifi.components.PropertyDescriptor.{ Builder => PropertyDescriptorBuilder }
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.{Map => JavaMap}
import java.util.{Set => JavaSet}
import scala.collection.JavaConverters._
import org.apache.nifi.components.Validator


object ProcessorUtils {
  val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorUtils])
  
  def generatePropertyDescriptors(propDescMap: Map[String, Map[String, String]]): List[PropertyDescriptor] = {
    var propertyDescriptors: List[PropertyDescriptor] = Nil
    for ((k, v) <-propDescMap) {
      val propDescBuilder: PropertyDescriptorBuilder = new PropertyDescriptorBuilder()
      v.foreach { x =>
        addPropertyDescriptor(propDescBuilder, x._1, x._2)
      }
      propDescBuilder.addValidator(Validator.VALID)
      propertyDescriptors = propDescBuilder.build() :: propertyDescriptors
    }
    propertyDescriptors
  }

  def addPropertyDescriptor(propDescBuilder: PropertyDescriptorBuilder,
                            key: String,
                            value: String) = key match {
    case FlowModule.PropertyName => propDescBuilder.name(value)
    case FlowModule.PropertyDescription=> propDescBuilder.description(value)
    case FlowModule.PropertyRequired => propDescBuilder.required(value.toBoolean)
    case FlowModule.PropertyDefaultValue => propDescBuilder.defaultValue(value)
    case _ => logger.warn("Relationship " + key + " not known")
  }

  def generateRelationships(relationshipMap: Map[String, Map[String, String]]): Set[Relationship] = {
    var relationships: Set[Relationship] = Set()
    for ((k, v) <- relationshipMap) {
      val relationshipBuilder: RelationshipBuilder = new RelationshipBuilder()
      v.foreach { x =>
        addRelationship(relationshipBuilder, x._1, x._2)
      }
      relationships += relationshipBuilder.build()
    }
    relationships
  }

  def addRelationship(relationshipBuilder: RelationshipBuilder,
                      key: String,
                      value: String) = key match {
    case FlowModule.RelationshipName => relationshipBuilder.name(value)
    case _                                     => logger.warn("Relationship " + key + " not known")

  }

  def valueProperties(propertyDescriptorValueMap: JavaMap[PropertyDescriptor, String]): Map[String, String] = {    
    propertyDescriptorValueMap.asScala.map(x => (x._1.getName, x._2)).toMap    
  }

  def successRelationship(relationships: JavaSet[Relationship]): Option[Relationship] = {
    return relationships.asScala.find(FlowModule.RelSuccessId == _.getName)
  }
}

class ProcessorUtils {

}