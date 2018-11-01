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

import org.dcs.commons.serde.AvroSchemaStore

class TestProcessorISpec extends ProcessorsBaseUnitSpec with TestProcessorBehaviors {
  import TestProcessorSpec._

  "Test Processor Response" must " be valid " taggedAs(IT) in {
    AvroSchemaStore.add(TestResponseSchemaId)
    val testProcessor: WorkerProcessor = new WorkerProcessor()
    testProcessor.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)
    validResponse(testProcessor)
  }

  override def processorServiceClassName: String = ProcessorServiceClassName
}