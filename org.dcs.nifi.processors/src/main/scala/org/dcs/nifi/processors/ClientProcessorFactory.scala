package org.dcs.nifi.processors

import org.apache.nifi.components.PropertyDescriptor.{Builder => PropertyDescriptorBuilder}
import org.apache.nifi.components.{AllowableValue, PropertyDescriptor, Validator}
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.Relationship.{Builder => RelationshipBuilder}
import org.apache.nifi.processor.util.StandardValidators
import org.dcs.api.processor.{PossibleValue, PropertyLevel, PropertyType, RemoteProperty, RemoteRelationship}

import scala.collection.JavaConverters._

/**
  * Created by cmathew on 30/08/16.
  */
object PropertyDescriptor {

  def apply(remoteProperty: RemoteProperty): PropertyDescriptor = {

    val propDescBuilder: PropertyDescriptorBuilder = new PropertyDescriptorBuilder()

    propDescBuilder.displayName(remoteProperty.displayName)
    propDescBuilder.name(remoteProperty.name)
    propDescBuilder.description(remoteProperty.description)
    if(!(remoteProperty.defaultValue == null))
      propDescBuilder.defaultValue(remoteProperty.defaultValue)
    val possibleValues = remoteProperty.possibleValues.asScala.map(pv => pv.value).asJava
    if(possibleValues != null && !possibleValues.isEmpty)
      propDescBuilder.allowableValues(possibleValues)
    propDescBuilder.required(remoteProperty.required)
    propDescBuilder.sensitive(remoteProperty.sensitive)
    propDescBuilder.dynamic(remoteProperty.dynamic)
    remoteProperty.validators.asScala.map(v => propDescBuilder.addValidator(Class.forName(v).asInstanceOf[Validator]) )

    if(remoteProperty.required)
      propDescBuilder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    else
      propDescBuilder.addValidator(Validator.VALID)
    propDescBuilder.build()
  }

  def processorClassPd(): PropertyDescriptor = {
    val propDescBuilder: PropertyDescriptorBuilder = new PropertyDescriptorBuilder()
    propDescBuilder.displayName(RemoteProcessorClassKey)
    propDescBuilder.name(RemoteProcessorClassKey)
    propDescBuilder.description("Remote Processor class")
    propDescBuilder.defaultValue("")
    propDescBuilder.required(true)
    propDescBuilder.addValidator(Validator.VALID)
    propDescBuilder.build()
  }
}

object Relationship {
  def apply(remoteRelationship: RemoteRelationship): Relationship = {
   val relationshipBuilder = new RelationshipBuilder()

    relationshipBuilder.name(remoteRelationship.id)
    relationshipBuilder.description(remoteRelationship.description)

    relationshipBuilder.build()
  }
}

object RemoteProperty {
  def apply(propertyDescriptor: PropertyDescriptor): RemoteProperty  = {

    org.dcs.api.processor.RemoteProperty(propertyDescriptor.getDisplayName,
      propertyDescriptor.getName,
      propertyDescriptor.getDescription,
      propertyDescriptor.getDefaultValue,
      {
        val allowableValues = propertyDescriptor.getAllowableValues
        if(allowableValues == null || allowableValues.isEmpty)
          null
        else
        allowableValues.asScala.to[Set].map(av => PossibleValue(av)).asJava
      },
      propertyDescriptor.isRequired,
      propertyDescriptor.isSensitive,
      propertyDescriptor.isDynamic,
      propertyDescriptor.getValidators.asScala.map(v => v.getClass.getName).asJava,
      PropertyType.String,
      PropertyLevel.Open)


  }
}

object PossibleValue {
  def apply(allowableValue: AllowableValue): PossibleValue = {
    org.dcs.api.processor.PossibleValue(allowableValue.getValue, allowableValue.getDisplayName, allowableValue.getDescription)
  }
}

