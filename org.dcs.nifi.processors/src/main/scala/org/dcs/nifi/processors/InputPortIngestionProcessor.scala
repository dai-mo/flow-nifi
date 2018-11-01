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

import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import java.util.{Map => JavaMap}

import org.dcs.api.processor.{CoreProperties, RelationshipType, RemoteProcessor, RemoteRelationship}
import org.dcs.commons.serde.AvroImplicits._
import scala.collection.JavaConverters._

@Tags(Array("port-ingestion", "stateless"))
@CapabilityDescription("Stub for a remote ingestion processor connected to an input port")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
class InputPortIngestionProcessor extends InputOutputClientProcessor {

  override def output(in: Option[Array[Byte]],
             valueProperties: JavaMap[String, String]): Array[Array[Byte]] = in match {
    case None => Array()
    case Some(input) => {
      val inputString = new String(input)
      Array(RelationshipType.Success.id.getBytes(),
        inputString.serToBytes(CoreProperties(valueProperties.asScala.toMap).resolveReadSchema))
    }
  }
}
