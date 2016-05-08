package org.dcs.nifi.processors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.Relationship;
import org.dcs.api.service.FlowModuleConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorUtils {

	private static final Logger logger = LoggerFactory.getLogger(ProcessorUtils.class);

	public static List<PropertyDescriptor> generatePropertyDescriptors(Map<String, Properties> propertiesMap) {
		List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
		for(Properties props : propertiesMap.values()) {
			Builder propertyBuilder = new PropertyDescriptor.Builder();
			for(Map.Entry<Object, Object> entrySet : props.entrySet()) {
				addPropertyToDescriptor(propertyBuilder, entrySet.getKey().toString(), entrySet.getValue().toString());
			}
			propertyBuilder.addValidator(Validator.VALID);
			propertyDescriptors.add(propertyBuilder.build());
		}
		return propertyDescriptors;
	}

	public static void addPropertyToDescriptor(Builder propertyBuilder, String key, String value) {
		switch(key) {
		case FlowModuleConstants.PROPERTY_NAME:
			propertyBuilder.name(value);
			break;
		case FlowModuleConstants.PROPERTY_DESCRIPTION:
			propertyBuilder.description(value);
			break;
		case FlowModuleConstants.PROPERTY_REQUIRED:
			propertyBuilder.required(Boolean.valueOf(value));
			break;
		case FlowModuleConstants.PROPERTY_DEFAULT_VALUE:
			propertyBuilder.defaultValue(value);
			break;
		default:
			logger.warn("Property " + key + " not known");
		}	
	}

	public static Set<Relationship> generateRelationships(Map<String, Properties> relationshipMap) {
		Set<Relationship> relationships = new HashSet<>();
		for(Properties props : relationshipMap.values()) {
			org.apache.nifi.processor.Relationship.Builder relationshipBuilder = new Relationship.Builder();
			for(Map.Entry<Object, Object> entrySet : props.entrySet()) {
				addRelationship(relationshipBuilder, entrySet.getKey().toString(), entrySet.getValue().toString());
			}			
			relationships.add(relationshipBuilder.build());
		}
		return relationships;
	}
	
	public static void addRelationship(org.apache.nifi.processor.Relationship.Builder relationshipBuilder, 
			String key, 
			String value) {
		switch(key) {
		case FlowModuleConstants.RELATIONSHIP_NAME:
			relationshipBuilder.name(value);
			break;
		default:
			logger.warn("Relationship " + key + " not known");
		}	
	}
	
	public static Properties generateValueProperties(Map<PropertyDescriptor, String> propertyDescriptorValueMap) {
		Properties propertiesValueMap = new Properties();
		for(Map.Entry<PropertyDescriptor, String> propertyDescriptorValue : propertyDescriptorValueMap.entrySet()) {
			propertiesValueMap.setProperty(propertyDescriptorValue.getKey().getName(), propertyDescriptorValue.getValue());
		}
		return propertiesValueMap;
	}
	
	public static Relationship getSuccessRelationship(Set<Relationship> relationships) {
		for(Relationship relationship : relationships) {
			if(FlowModuleConstants.REL_SUCCESS_ID.equals(relationship.getName())) {
				return relationship;
			}
		}
		return null;
	}
}
