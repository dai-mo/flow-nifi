package org.dcs.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.dcs.api.service.ModuleFactoryService;
import org.dcs.api.service.RESTException;
import org.dcs.remote.RemoteService;


public abstract class RemoteProcessor extends AbstractProcessor {

	private ProcessorLog log = this.getLogger();

	private String flowModuleId;

	private ModuleFactoryService moduleFactoryService;
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;

	protected abstract String getFlowModuleClassName();
	
	@Override
	public void init(final ProcessorInitializationContext context){
		log = this.getLogger();		

		RemoteService.initialize("localhost:2181");
		moduleFactoryService = (ModuleFactoryService) RemoteService.getService(ModuleFactoryService.class);
		flowModuleId = moduleFactoryService.createFlowModule(getFlowModuleClassName());
		
		log.warn("Created flow module " + getFlowModuleClassName() + " with id " + flowModuleId);
		
		Map<String, Properties> propertiesMap = moduleFactoryService.getPropertyDescriptors(flowModuleId);
		properties = ProcessorUtils.generatePropertyDescriptors(propertiesMap);
		
		log.warn("Generated property descriptors ");
		
		Map<String, Properties> relationshipMap = moduleFactoryService.getRelationships(flowModuleId);
		relationships = ProcessorUtils.generateRelationships(relationshipMap);
		
		log.warn("Generated relationships ");
		
	}

	@OnScheduled
  public void onScheduled(final ProcessContext context) {

	}
  
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final Properties valueProperties = ProcessorUtils.generateValueProperties(context.getProperties());
    FlowFile flowFile = session.create();
    flowFile = session.write(flowFile, new OutputStreamCallback() {
        @Override
        public void process(final OutputStream out) throws IOException {
        		byte[] response;
						try {
							response = moduleFactoryService.trigger(flowModuleId, valueProperties);
						} catch (RESTException e) {
							throw new IOException(e);
						}
            out.write(response);
        }
    });

    final Map<String, String> attributes = new HashMap<>();
    attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
    attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
    flowFile = session.putAllAttributes(flowFile, attributes);

    Relationship successRelationship = ProcessorUtils.getSuccessRelationship(relationships);
    if(successRelationship != null) {
    	session.transfer(flowFile, successRelationship);
    }
                                                                                                                                                                                                                                                                                                                                                                                             
	}

	@Override
	public Set<Relationship> getRelationships(){
		return relationships;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
		return properties;
	}

}

