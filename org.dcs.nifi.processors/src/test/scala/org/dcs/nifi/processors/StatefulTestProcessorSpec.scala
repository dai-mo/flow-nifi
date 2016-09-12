package org.dcs.nifi.processors

import java.util.UUID

import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.RelationshipType
import org.dcs.remote.RemoteService
import org.mockito.Mockito._
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

object StatefulTestProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker
  val clientProcessor: StatefulTestProcessor = spy(new StatefulTestProcessor())
  doReturn(MockRemoteService).
    when(clientProcessor).
    remoteService

  val remoteProcessor: org.dcs.core.processor.StatefulTestProcessor = new org.dcs.core.processor.StatefulTestProcessor()

  val response: String = "{\"id\":" + UUID.randomUUID().toString+  "\"response\":\"Hello Bob\"}"
  MockZookeeperServiceTracker.addProcessor(
    clientProcessor.processorClassName(),
    new MockStatefulRemoteProcessorService(remoteProcessor, response.getBytes)
  )


}

class StatefulTestProcessorSpec extends ProcessorsBaseUnitSpec with StatefulTestProcessorBehaviors {

  import org.dcs.nifi.processors.StatefulTestProcessorSpec._

  "Test Processor Response" must " be valid " in {
    validResponse(clientProcessor)
  }
}

trait StatefulTestProcessorBehaviors {
  this: FlatSpec =>

  def validResponse(testProcessor: StatefulTestProcessor): String =  {

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)
    val user = "Bob"


    // Add properties
    runner.setProperty("user", user)

    runner.enqueue("Hello ".getBytes)
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    var successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.SucessRelationship)

    val results1: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results1.size == 1)
    val result1: MockFlowFile = results1.get(0)
    val resultValue1: String = new String(runner.getContentAsByteArray(result1))

    runner.enqueue("Hello ".getBytes)
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.SucessRelationship)

    val results2: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results2.size == 2)
    val result2: MockFlowFile = results2.get(1)
    val resultValue2: String = new String(runner.getContentAsByteArray(result2))
    assert(resultValue1 == resultValue2)
    resultValue1
  }

}