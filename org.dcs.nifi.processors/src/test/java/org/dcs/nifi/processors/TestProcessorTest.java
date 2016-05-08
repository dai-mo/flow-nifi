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
package org.dcs.nifi.processors;

import java.io.IOException;
import java.util.List;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcs.remote.RemoteService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestProcessorTest {

    private TestRunner testRunner;

    @BeforeClass
    public static void setup() {
    	RemoteService.initialize("localhost:2181");
    }
    
    @AfterClass
    public static void dispose() {
    	RemoteService.close();
    }
    
    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(MyProcessor.class);
    }

    @Test
    public void testProcessor() throws InitializationException, IOException {
      
    	TestProcessor testProcessor = new TestProcessor();
      // Generate a test runner to mock a processor in a flow
      TestRunner runner = TestRunners.newTestRunner(testProcessor);
      
      String user = "Bob";
      // Add properites
      runner.setProperty("User Name", user);

      // Run the enqueued content, it also takes an int = number of contents queued
      runner.run(1);    

      Relationship successRelationship = ProcessorUtils.getSuccessRelationship(testProcessor.getRelationships());
//      // If you need to read or do aditional tests on results you can access the content
      List<MockFlowFile> results = runner.getFlowFilesForRelationship(successRelationship);
      Assert.assertTrue("1 match", results.size() == 1);
      MockFlowFile result = results.get(0);
      String resultValue = new String(runner.getContentAsByteArray(result));
      Assert.assertEquals("Hello " + user + "! This is DCS", resultValue);

    }

}
