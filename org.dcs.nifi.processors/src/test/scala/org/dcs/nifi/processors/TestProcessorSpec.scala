package org.dcs.nifi.processors

import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.dcs.core.module.flow.TestFlowModule
import org.dcs.nifi.processors.TestProcessorSpec.testProcessor
import org.dcs.remote.RemoteService
import org.scalatest.FlatSpec

object TestProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker
  MockZookeeperServiceTracker.addService(
    "org.dcs.api.service.ModuleFactoryService",
    new MockModuleFactoryService(new TestFlowModule, "Hello Bob! This is DCS"))

  val testProcessor: TestProcessor = new TestProcessor()
  testProcessor.remoteService = MockRemoteService
}

class TestProcessorSpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {

  "Test Processor Response" must " be valid " in {
    validResponse(testProcessor)
  }
}

trait TestProcessorBehaviors { this: FlatSpec =>

  def validResponse(testProcessor: TestProcessor) {

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
    assert(results.size == 1)
    val result: MockFlowFile = results.get(0);
    val resultValue: String = new String(runner.getContentAsByteArray(result));
    assert(resultValue == "Hello " + user + "! This is DCS")
  }

}