package org.dcs.nifi.processors

import java.io.OutputStream

import org.apache.avro.generic.GenericData
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.{RelationshipType, RemoteProcessor}
import org.dcs.remote.RemoteService
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore

import scala.collection.JavaConverters._
import scala.collection.mutable

object TestProcessorSpec {
  object MockRemoteService extends RemoteService with MockZookeeperServiceTracker
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
    // Add file attributes
    val attributes = new java.util.HashMap[String, String]()
    val schemaId = "org.dcs.core.processor.TestRequestProcessor"
    attributes.put(RemoteProcessor.SchemaIdKey, schemaId)

    val schema = AvroSchemaStore.get("org.dcs.core.processor.TestRequestProcessor")
    val record = new GenericData.Record(schema.get)
    record.put("request", "Hello ")
    runner.enqueue(record.serToBytes(schema), attributes)
    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    val successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.SucessRelationship)

    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results.size == 1)
    val result: MockFlowFile = results.get(0)

    val schemaResponse = AvroSchemaStore.get("org.dcs.core.processor.TestResponseProcessor")
    val expectedRecord = new GenericData.Record(schemaResponse.get)
    expectedRecord.put("response", "Hello " + user)
    val actualRecord = runner.getContentAsByteArray(result).deSerToGenericRecord(schemaResponse, schemaResponse)
    assert(actualRecord == expectedRecord)
  }

}