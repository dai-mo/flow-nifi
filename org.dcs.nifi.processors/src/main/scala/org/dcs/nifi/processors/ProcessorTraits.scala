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

import java.io.{InputStream, OutputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.{List => JavaList, Map => JavaMap, Set => JavaSet}

import org.apache.commons.io.IOUtils
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


trait Write {
  def writeCallback(flowFile: FlowFile,
                    session: ProcessSession,
                    toWrite: Array[Byte]): FlowFile = {
   session.write(flowFile, new OutputStreamCallback() {
      override def process(out: OutputStream) {
        out.write(toWrite)
      }
    })
  }
}

trait Read {
  def readCallback(flowFile: FlowFile,
                    session: ProcessSession,
                    toRead: AtomicReference[Array[Byte]]) = {
    session.read(flowFile, new InputStreamCallback() {
      override def process(in: InputStream) {
        toRead.set(IOUtils.toByteArray(in))
      }
    })
  }
}


@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
trait InputOutputClientProcessor extends ClientProcessor {
  override def canRead: Boolean = true
  override def canWrite: Boolean = true
}

@InputRequirement(Requirement.INPUT_REQUIRED)
trait InputOutputStatefulClientProcessor extends StatefulClientProcessor {
  override def canRead: Boolean = true
  override def canWrite: Boolean = true
}

@SideEffectFree
@InputRequirement(Requirement.INPUT_FORBIDDEN)
trait OutputClientProcessor extends ClientProcessor {
  override def canRead: Boolean = false
  override def canWrite: Boolean = true
}

@InputRequirement(Requirement.INPUT_FORBIDDEN)
trait OutputStatefulClientProcessor extends StatefulClientProcessor {
  override def canRead: Boolean = false
  override def canWrite: Boolean = true
}

@InputRequirement(Requirement.INPUT_FORBIDDEN)
trait NoInputOutputStatefulClientProcessor extends StatefulClientProcessor {
  override def canRead: Boolean = false
  override def canWrite: Boolean = false
}
