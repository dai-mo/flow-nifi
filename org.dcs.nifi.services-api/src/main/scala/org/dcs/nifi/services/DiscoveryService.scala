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

package org.dcs.nifi.services

import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.controller.ControllerService
import org.apache.nifi.processor.exception.ProcessException
import scala.reflect.ClassTag

@Tags(Array("discovery"))
@CapabilityDescription("Provides the ability to discover remote services via "
		+ " service brokers like zookeeper")
trait DiscoveryService extends ControllerService {  
	
	def service[T](implicit tag: ClassTag[T]): T

}