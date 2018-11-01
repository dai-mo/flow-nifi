/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
  val ProcessorServiceClassName = "org.dcs.core.service.TestProcessorService"

  val TestRequestSchemaId = "org.dcs.core.processor.TestRequest"
  val TestResponseSchemaId = "org.dcs.core.processor.TestResponse"

}

class TestProcessorSpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {
  import TestProcessorSpec._

  "Test Processor Response" should " be valid " in {
    val clientProcessor = mockClientProcessor(new org.dcs.core.processor.TestProcessor(),
      Array("{\"response\":\"Hello Bob\"}".getBytes))
    clientProcessor.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)
    validResponse(clientProcessor)
  }

  override def processorServiceClassName: String = ProcessorServiceClassName
}

trait TestProcessorBehaviors {
  import TestProcessorSpec._

  def validResponse(testProcessor: ClientProcessor) {

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

    val successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.Success.id)

    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results.size == 1)
    val result: MockFlowFile = results.get(0)
    results.asScala.foreach(ff =>
      assert(ff.getAttributes.get(Attributes.RelationshipAttributeKey) == RelationshipType.Success.id))

    val testResponseSchema = AvroSchemaStore.get(TestResponseSchemaId)

    val expectedRecord = new GenericData.Record(testResponseSchema.get)
    expectedRecord.put("response", "Hello " + user)
    val actualRecord = runner.getContentAsByteArray(result).deSerToGenericRecord(testResponseSchema, testResponseSchema)
    assert(actualRecord == expectedRecord)
  }

}