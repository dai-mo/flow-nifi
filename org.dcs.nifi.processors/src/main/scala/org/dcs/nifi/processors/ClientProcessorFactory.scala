package org.dcs.nifi.processors

import org.apache.nifi.components.PropertyDescriptor.{Builder => PropertyDescriptorBuilder}
import org.apache.nifi.components.{AllowableValue, PropertyDescriptor, Validator}
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.Relationship.{Builder => RelationshipBuilder}
import org.dcs.api.processor.{PossibleValue, PropertyType, RemoteProperty, RemoteRelationship}

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
    propDescBuilder.defaultValue(remoteProperty.defaultValue)
    propDescBuilder.allowableValues(remoteProperty.possibleValues.asScala.map(pv => pv.value).asJava)
    propDescBuilder.required(remoteProperty.required)
    propDescBuilder.sensitive(remoteProperty.sensitive)
    propDescBuilder.dynamic(remoteProperty.dynamic)
    remoteProperty.validators.asScala.map(v => propDescBuilder.addValidator(Class.forName(v).asInstanceOf[Validator]) )

    propDescBuilder.addValidator(Validator.VALID)
    propDescBuilder.build()
  }
}

object Relationship {
  def apply(remoteRelationship: RemoteRelationship): Relationship = {
   val relationshipBuilder = new RelationshipBuilder()

    relationshipBuilder.name(remoteRelationship.name)
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
      propertyDescriptor.getAllowableValues.asScala.to[Set].map(av => PossibleValue(av)).asJava,
      propertyDescriptor.isRequired,
      propertyDescriptor.isSensitive,
      propertyDescriptor.isDynamic,
      propertyDescriptor.getValidators.asScala.map(v => v.getClass.getName).asJava,
      PropertyType.String)


  }
}

object PossibleValue {
  def apply(allowableValue: AllowableValue): PossibleValue = {
    org.dcs.api.processor.PossibleValue(allowableValue.getValue, allowableValue.getDisplayName, allowableValue.getDescription)
  }
}

