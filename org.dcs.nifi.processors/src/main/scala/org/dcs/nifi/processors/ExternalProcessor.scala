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
import org.apache.nifi.processor.{ProcessContext, ProcessSession}

@Tags(Array("external", "stateful"))
@CapabilityDescription("Stub for a remote external processor")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
class ExternalProcessor extends NoInputOutputStatefulClientProcessor {

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    context.`yield`()
  }
}
