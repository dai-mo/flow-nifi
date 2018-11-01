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



import org.apache.nifi.util.{MockFlowFile, TestRunner, TestRunners}
import org.dcs.api.processor.RelationshipType
import org.dcs.remote.RemoteService
import org.mockito.Mockito._
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore

object StatefulGBIFOccurrenceProcessorSpec {
  val ProcessorServiceClassName = "org.dcs.core.service.StatefulGBIFOccurrenceProcessorService"
}

class StatefulGBIFOccurrenceProcessorSpec extends ProcessorsBaseUnitSpec with StatefulGBIFOccurrenceProcessorBehaviors {

  import org.dcs.nifi.processors.StatefulGBIFOccurrenceProcessorSpec._

  override def processorServiceClassName: String = ProcessorServiceClassName

  // FIXME: Setup sample GBIF output to run unit test
  //  "Stateful GBIF Occurrence Processor Response" must " be valid " in {
  // val clientProcessor = mockClientProcessor(new org.dcs.core.processor.GBIFOccurrenceProcessor(),
  // Array("".getBytes()))
  // clientProcessor.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)
  //    validResponse(clientProcessor)
  //  }
}

trait StatefulGBIFOccurrenceProcessorBehaviors {
  this: FlatSpec =>
  import StatefulTestProcessorSpec._

  def validResponse(testProcessor: IngestionStatefulProcessor) =  {

    // Generate a test runner to mock a processor in a flow
    val runner: TestRunner = TestRunners.newTestRunner(testProcessor)
    val user = "Bob"


    // Add properties
    runner.setProperty("species-name", "Loxodonta africana")

    // Run the enqueued content, it also takes an int = number of contents queued
    runner.run(1)

    val successRelationship = testProcessor.getRelationships().asScala.find(r => r.getName == RelationshipType.Success.id)

    val results: java.util.List[MockFlowFile] = runner.getFlowFilesForRelationship(successRelationship.get)
    assert(results.size == 200)
    AvroSchemaStore.add(TestResponseSchemaId)
    val schema = AvroSchemaStore.get(TestResponseSchemaId)
    results.asScala.foreach(result => {
      val gbifRecord = runner.getContentAsByteArray(result).deSerToGenericRecord(schema, schema)
    })


  }

}