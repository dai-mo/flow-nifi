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
