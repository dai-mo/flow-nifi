package org.dcs.nifi.processors



import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.RelationshipType
import org.dcs.remote.RemoteService
import org.mockito.Mockito._
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import org.dcs.commons.serde.AvroImplicits._

object StatefulGBIFOccurrenceProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker

  val clientProcessor: StatefulGBIFOccurrenceProcessor = spy(new StatefulGBIFOccurrenceProcessor())
  doReturn(MockRemoteService).
    when(clientProcessor).
    remoteService

  val remoteProcessor: org.dcs.core.processor.StatefulTestProcessor = new org.dcs.core.processor.StatefulTestProcessor()

  val response: Array[Array[Byte]] = Array("".getBytes())
  MockZookeeperServiceTracker.addProcessor(
    clientProcessor.processorClassName(),
    new MockStatefulRemoteProcessorService(remoteProcessor, response)
  )


}

class StatefulGBIFOccurrenceProcessorSpec extends ProcessorsBaseUnitSpec with StatefulGBIFOccurrenceProcessorBehaviors {

  import org.dcs.nifi.processors.StatefulGBIFOccurrenceProcessorSpec._

  // FIXME: Setup sample GBIF output to run unit test
//  "Stateful GBIF Occurrence Processor Response" must " be valid " in {
//    validResponse(clientProcessor)
//  }
}

trait StatefulGBIFOccurrenceProcessorBehaviors {
  this: FlatSpec =>

  def validResponse(testProcessor: StatefulGBIFOccurrenceProcessor) =  {

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)
    val user = "Bob"


    // Add properties
    runner.setProperty("species-name", "Loxodonta africana")

    //runner.enqueue("Hello ".getBytes)
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    val successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.SucessRelationship)

    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results.size == 10)
    results.asScala.foreach(result => {
      assert(runner.getContentAsByteArray(result).deSerToJsonMap()("genus").asInstanceOf[String] == "Loxodonta")
    })


  }

}