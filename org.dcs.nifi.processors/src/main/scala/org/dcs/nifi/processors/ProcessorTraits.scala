package org.dcs.nifi.processors

import java.io.{InputStream, OutputStream}
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.nifi.annotation.behavior.{InputRequirement, SideEffectFree}
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.flowfile.attributes.CoreAttributes
import org.apache.nifi.processor.{ProcessSession, Relationship}
import org.apache.nifi.processor.io.{InputStreamCallback, OutputStreamCallback, StreamCallback}
import org.dcs.api.processor.{Configuration, RelationshipType}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Created by cmathew on 07/09/16.
  */
trait WriteOutput {
  def output(in: Option[InputStream], valueProperties: JavaMap[String, String]): JavaList[Array[Byte]]

  def flush(flowFile: FlowFile,
            session: ProcessSession,
            configuration: Configuration,
            relationships: JavaSet[Relationship]) = {
    val attributes = mutable.Map[String, String]()

    attributes(CoreAttributes.MIME_TYPE.key()) = configuration.outputMimeType

    val updatedFlowFile = session.putAllAttributes(flowFile, attributes.asJava)

    val successRelationship: Option[Relationship] =
      relationships.asScala.find(r => r.getName == RelationshipType.SucessRelationship)
    if (successRelationship.isDefined) {
      session.transfer(updatedFlowFile, successRelationship.get)
    }
  }
}

trait IO {
  def writeCallback(flowFile: FlowFile,
                    valueProperties: JavaMap[String, String],
                    session: ProcessSession,
                    configuration: Configuration,
                    relationships: JavaSet[Relationship]): FlowFile
}

trait InputOutput extends IO with WriteOutput{
  def writeCallback(flowFile: FlowFile,
                    valueProperties: JavaMap[String, String],
                    session: ProcessSession,
                    configuration: Configuration,
                    relationships: JavaSet[Relationship]): FlowFile = {
   session.write(flowFile, new StreamCallback() {
      override def process(in: InputStream, out: OutputStream) {
        output(Some(in), valueProperties).asScala.foreach { result =>
          out.write(result)
          flush(flowFile, session, configuration, relationships)
        }
      }
    })
  }
}

trait Output extends IO with WriteOutput{
  def writeCallback(flowFile: FlowFile,
                    valueProperties: JavaMap[String, String],
                    session: ProcessSession,
                    configuration: Configuration,
                    relationships: JavaSet[Relationship]): FlowFile = {
    session.write(flowFile, new OutputStreamCallback() {
      override def process(out: OutputStream) {
        output(None, valueProperties).asScala.foreach { result =>
          out.write(result)
          flush(flowFile, session, configuration, relationships)
        }
      }
    })
  }
}


@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
trait InputOutputClientProcessor extends ClientProcessor with InputOutput

@InputRequirement(Requirement.INPUT_REQUIRED)
trait InputOutputStatefulClientProcessor extends StatefulClientProcessor with InputOutput

@SideEffectFree
@InputRequirement(Requirement.INPUT_FORBIDDEN)
trait OutputClientProcessor extends ClientProcessor with Output

@InputRequirement(Requirement.INPUT_FORBIDDEN)
trait OutputStatefulClientProcessor extends StatefulClientProcessor with Output
