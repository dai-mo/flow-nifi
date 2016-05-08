package org.dcs.nifi.processors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ProcessorMockFactory {
	
  public static ConfigurationContext getMockProcessContext(final PropertyDescriptor pd, 
  		final Map<PropertyDescriptor, String> properties){

  	ConfigurationContext configurationContext = mock(ConfigurationContext.class);
    when(configurationContext.getProperty(pd)).thenAnswer(new Answer<PropertyValue>() {     

      @Override
      public PropertyValue answer(InvocationOnMock invocation) throws Throwable {
      	return new StandardPropertyValue(properties.get(pd), null);
      }
    });
    return configurationContext;
  }

}
