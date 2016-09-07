package org.dcs.nifi.processors

import java.io.{InputStream, OutputStream}
import java.util.{Map => JavaMap}

import org.apache.nifi.annotation.behavior.{InputRequirement, SideEffectFree}
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.io.{InputStreamCallback, OutputStreamCallback, StreamCallback}

/**
  * Created by cmathew on 07/09/16.
  */
trait WriteOutput {
  def output(in: Option[InputStream], valueProperties: JavaMap[String, String]): Array[Byte]
}

trait IO {
  def writeCallback(flowFile: FlowFile,
                    valueProperties: JavaMap[String, String],
                    session: ProcessSession): FlowFile
}

trait InputOutput extends IO with WriteOutput{
  def writeCallback(flowFile: FlowFile, valueProperties: JavaMap[String, String], session: ProcessSession): FlowFile = {
    session.write(flowFile, new StreamCallback() {
      override def process(in: InputStream, out: OutputStream) {
        out.write(output(Some(in), valueProperties))
      }
    })
  }
}

trait Output extends IO with WriteOutput{
  def writeCallback(flowFile: FlowFile, valueProperties: JavaMap[String, String], session: ProcessSession): FlowFile = {
    session.write(flowFile, new OutputStreamCallback() {
      override def process(out: OutputStream) {
        out.write(output(None, valueProperties))
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
