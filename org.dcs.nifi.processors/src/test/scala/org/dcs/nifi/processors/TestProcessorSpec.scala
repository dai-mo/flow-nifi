package org.dcs.nifi.processors

import org.apache.avro.generic.GenericData
import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.CoreProperties._
import org.dcs.api.processor.{Attributes, RelationshipType, RemoteProperty}
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore
import org.dcs.remote.RemoteService
import org.mockito.Mockito._

import scala.collection.JavaConverters._


object TestProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker

  val TestRequestSchemaId = "org.dcs.core.processor.TestRequest"
  val TestResponseSchemaId = "org.dcs.core.processor.TestResponse"

  val clientProcessor: TestProcessor = spy(new TestProcessor())

  doReturn(MockRemoteService).
    when(clientProcessor).
    remoteService

  val remoteProcessor: org.dcs.core.processor.TestProcessor = new org.dcs.core.processor.TestProcessor()

  val response: Array[Array[Byte]] = Array("{\"response\":\"Hello Bob\"}".getBytes)

  MockZookeeperServiceTracker.addProcessor(
    clientProcessor.processorClassName(),
    new MockRemoteProcessorService(remoteProcessor, response)
  )



}

class TestProcessorSpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {

  import TestProcessorSpec._

  "Test Processor Response" should " be valid " in {

    validResponse(clientProcessor)
  }
}

trait TestProcessorBehaviors {
  import TestProcessorSpec._

  def validResponse(testProcessor: TestProcessor) {

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)
    val user = "Bob"

    AvroSchemaStore.add(TestRequestSchemaId)
    val testRequestSchema = AvroSchemaStore.get(TestRequestSchemaId)

    // Add properties
    runner.setProperty("user", user)
    runner.setProperty(ReadSchemaIdKey, TestRequestSchemaId)

    val record = new GenericData.Record(testRequestSchema.get)
    record.put("request", "Hello ")
    runner.enqueue(record.serToBytes(testRequestSchema))
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    val successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.SucessRelationship)

    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results.size == 1)
    val result: MockFlowFile = results.get(0)
    results.asScala.foreach(ff =>
      assert(ff.getAttributes.get(Attributes.RelationshipAttributeKey) == RelationshipType.SucessRelationship))


    val testResponseSchema = AvroSchemaStore.get(TestResponseSchemaId)

    val expectedRecord = new GenericData.Record(testResponseSchema.get)
    expectedRecord.put("response", "Hello " + user)
    val actualRecord = runner.getContentAsByteArray(result).deSerToGenericRecord(testResponseSchema, testResponseSchema)
    assert(actualRecord == expectedRecord)
  }

}