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

/**
  * Created by cmathew on 13.11.16.
  */

class StatefulGBIFOccurrenceProcessorISpec extends ProcessorsBaseUnitSpec with StatefulGBIFOccurrenceProcessorBehaviors {

  import StatefulGBIFOccurrenceProcessorSpec._

  "Stateful GBIF Occurrence Processor Response" must " be valid " taggedAs(IT) in {
    val gbifOccurrenceProcessor: IngestionStatefulProcessor = new IngestionStatefulProcessor()
    gbifOccurrenceProcessor.onPropertyModified(PropertyDescriptor.processorClassPd(), "", processorServiceClassName)
    validResponse(gbifOccurrenceProcessor)
  }

  override def processorServiceClassName: String = ProcessorServiceClassName
}



