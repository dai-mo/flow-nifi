/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dcs.nifi.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.dcs.remote.RemoteService;

@Tags({"discovery", "zookeeper"})
@CapabilityDescription("Provides the ability to discover remote services via zookeeper")
public class ZookeeperDiscoveryService extends AbstractControllerService implements DiscoveryService {

		public static final String DEFAULT_ZOOKEEPER_SERVER="localhost:2181";
	
    public static final PropertyDescriptor SERVERS = new PropertyDescriptor
            .Builder().name("Zookeeper Servers")
            .description("The (space separated) list of zookeeper servers in '<domain>:<port>' form")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(DEFAULT_ZOOKEEPER_SERVER)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SERVERS);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

  	@Override
  	public void init(final ControllerServiceInitializationContext config){
  		
  	}

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
    	String servers = context.getProperty(SERVERS).getValue();
    	RemoteService.initialize(servers);
    }

    @OnDisabled
    public void shutdown() {
    	RemoteService.close();
    }

		@Override
		public Object getService(Class<?> serviceClass) {
			return RemoteService.getService(serviceClass);
		}


}
