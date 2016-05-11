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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcs.api.model.TestResponse;
import org.dcs.api.service.FlowModule;
import org.dcs.api.service.ModuleFactoryService;
import org.dcs.api.service.RESTException;
import org.dcs.api.service.TestApiService;
import org.dcs.core.module.flow.TestFlowModule$;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME: Move this to scala when the discovery service is implemented correctly
@Ignore
public class ZookeeperDiscoveryServiceTest {

	private static final Logger logger = LoggerFactory.getLogger(ZookeeperDiscoveryServiceTest.class);
	
	
	@Before
	public void init() {

	}

	@Test
	public void testService() throws InitializationException, RESTException {
//		final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
//		final DiscoveryService discoveryService = new ZookeeperDiscoveryService();
//		runner.addControllerService("test-good", discoveryService);
//
//		
//		runner.enableControllerService(discoveryService);
//		runner.assertValid(discoveryService);
//
//		TestApiService testService = (TestApiService)discoveryService.service(scala.reflect.ClassTag$.MODULE$.apply(TestApiService.class));
//		
//		String user = "Bob";
//		TestResponse testResponse = testService.testHelloGet(user);
//		Assert.assertNotNull(testResponse);
//		String excepted = "Hello " + user + "! This is DCS";
//		Assert.assertEquals(excepted, testResponse.getResponse());
//		
//		
//		ModuleFactoryService mFactory = (ModuleFactoryService)discoveryService.service(scala.reflect.ClassTag$.MODULE$.apply(ModuleFactoryService.class));
//		
//		String moduleUUID = mFactory.createFlowModule("org.dcs.core.module.flow.TestFlowModule");
//		
//		Assert.assertNotNull(moduleUUID);
//		
//		
//		scala.collection.immutable.HashMap<String, String> valueProperties = new scala.collection.immutable.HashMap<String, String>();
//		valueProperties.put(TestFlowModule$.MODULE$.PropertyUserNameValue(), user);
//		
//		String testResponseStr = new String(mFactory.trigger(moduleUUID, valueProperties), StandardCharsets.UTF_8);
//		Assert.assertEquals(excepted, testResponseStr);
//		
//		mFactory.remove(moduleUUID);
//		
//		runner.disableControllerService(discoveryService);
	}

}
;