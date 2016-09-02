package org.dcs.nifi.processors

import org.apache.nifi.components.{PropertyDescriptor, Validator}
import org.apache.nifi.components.PropertyDescriptor.{Builder => PropertyDescriptorBuilder}
import org.apache.nifi.processor.Relationship.{Builder => RelationshipBuilder}
import org.apache.nifi.processor.Relationship
import org.dcs.api.processor.{PropertySettings, RelationshipSettings}

/**
  * Created by cmathew on 30/08/16.
  */
object PropertyDescriptor {
  def apply(propertySettings: PropertySettings): PropertyDescriptor = {

    val propDescBuilder: PropertyDescriptorBuilder = new PropertyDescriptorBuilder()

    propDescBuilder.displayName(propertySettings.displayName)
    propDescBuilder.name(propertySettings.name)
    propDescBuilder.description(propertySettings.description)
    propDescBuilder.defaultValue(propertySettings.defaultValue)

    propDescBuilder.addValidator(Validator.VALID)
    propDescBuilder.build()
  }
}

object Relationship {
  def apply(relationshipSettings: RelationshipSettings): Relationship = {
   val relationshipBuilder = new RelationshipBuilder()

    relationshipBuilder.name(relationshipSettings.name)
    relationshipBuilder.description(relationshipSettings.description)

    relationshipBuilder.build()
  }
}
