package org.dcs.nifi.processors

import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.RelationshipType
import org.dcs.remote.RemoteService
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

object TestProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker
  val clientProcessor: TestProcessor = new TestProcessor()
  clientProcessor.remoteService = MockRemoteService

  val remoteProcessor: org.dcs.core.processor.TestProcessor = new org.dcs.core.processor.TestProcessor()

  MockZookeeperServiceTracker.addProcessor(
    clientProcessor.processorClassName(),
    new MockRemoteProcessorService(remoteProcessor, "{\"response\":\"Hello Bob\"}".getBytes)
  )


}

class TestProcessorSpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {

  import org.dcs.nifi.processors.TestProcessorSpec._

  "Test Processor Response" must " be valid " in {
    validResponse(clientProcessor)
  }
}

trait TestProcessorBehaviors { this: FlatSpec =>

  def validResponse(testProcessor: TestProcessor) {

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)
    val user = "Bob"


    // Add properties
    runner.setProperty("user", user)
    runner.enqueue("Hello ".getBytes)
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    val successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.SucessRelationship)

    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results.size == 1)
    val result: MockFlowFile = results.get(0)
    val resultValue: String = new String(runner.getContentAsByteArray(result))
    assert(resultValue == "{\"response\":\"Hello " + user + "\"}")
  }

}