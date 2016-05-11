
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
	
	def service[T](implicit tag: ClassTag[T]): Option[T]

}