package org.dcs.nifi.processors

import org.dcs.nifi.procesors.ProcessorsBaseUnitSpec
import org.apache.nifi.util.TestRunners
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.MockFlowFile
import org.dcs.remote.ZookeeperServiceTracker
import org.dcs.remote.RemoteService
import org.dcs.core.module.flow.TestFlowModule

class TestProcessorSpec extends ProcessorsBaseUnitSpec {
  
  "The Test Processor " should " return the correct greeting" in {

    object MockRemoteService extends RemoteService with MockZookeeperServiceTracker
    MockZookeeperServiceTracker.addService(
        "org.dcs.api.service.ModuleFactoryService",
        new MockModuleFactoryService(new TestFlowModule, "Hello Bob! This is DCS")
        )
    
    val testProcessor: TestProcessor = new TestProcessor()
    testProcessor.remoteService = MockRemoteService

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)

    val user = "Bob";

    // Add properties
    runner.setProperty("User Name", user)

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    val successRelationship = ProcessorUtils.successRelationship(testProcessor.getRelationships())
    //      // If you need to read or do aditional tests on results you can access the content
    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    results.size should be(1)
    val result: MockFlowFile = results.get(0);
    val resultValue: String = new String(runner.getContentAsByteArray(result));
    resultValue should be("Hello " + user + "! This is DCS")
  }
}