package org.dcs.nifi.processors

import java.util.UUID

import org.apache.avro.generic.GenericData
import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.CoreProperties.ReadSchemaIdKey
import org.dcs.api.processor.{RelationshipType, RemoteProcessor}
import org.dcs.commons.serde.AvroSchemaStore
import org.dcs.remote.RemoteService
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.dcs.commons.serde.AvroImplicits._

import scala.collection.JavaConverters._

object StatefulTestProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker
  val clientProcessor: StatefulTestProcessor = spy(new StatefulTestProcessor())
  doReturn(MockRemoteService).
    when(clientProcessor).
    remoteService

  val remoteProcessor: org.dcs.core.processor.StatefulTestProcessor = new org.dcs.core.processor.StatefulTestProcessor()

  val response: Array[Array[Byte]] =
    Array(("{\"id\":" + UUID.randomUUID().toString +  "\"response\":\"Hello Bob\"}").getBytes())
  MockZookeeperServiceTracker.addProcessor(
    clientProcessor.processorClassName(),
    new MockStatefulRemoteProcessorService(remoteProcessor, response)
  )


}

class StatefulTestProcessorSpec extends ProcessorsBaseUnitSpec with StatefulTestProcessorBehaviors {

  import org.dcs.nifi.processors.StatefulTestProcessorSpec._

  "Stateful Test Processor Response" must " be valid " in {
    validResponse(clientProcessor)
  }
}

trait StatefulTestProcessorBehaviors {
  this: FlatSpec =>

  def validResponse(testProcessor: StatefulTestProcessor): String =  {

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)
    val user = "Bob"

    val testRequestSchemaId = "org.dcs.core.processor.TestRequest"
    AvroSchemaStore.add(testRequestSchemaId)
    val testRequestSchema = AvroSchemaStore.get(testRequestSchemaId)

    // Add properties
    runner.setProperty("user", user)
    runner.setProperty(ReadSchemaIdKey, testRequestSchemaId)

    val record = new GenericData.Record(testRequestSchema.get)
    record.put("request", "Hello ")
    runner.enqueue(record.serToBytes(testRequestSchema))

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    var successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.Success.id)

    val results1: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results1.size == 1)
    val result1: MockFlowFile = results1.get(0)
    val resultValue1: String = new String(runner.getContentAsByteArray(result1))

    runner.enqueue(record.serToBytes(testRequestSchema))
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.Success.id)

    val results2: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results2.size == 2)
    val result2: MockFlowFile = results2.get(1)
    val resultValue2: String = new String(runner.getContentAsByteArray(result2))
    assert(resultValue1 == resultValue2)
    resultValue1
  }

}