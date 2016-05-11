package org.dcs.nifi.services

import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.controller.ControllerServiceInitializationContext
import org.apache.nifi.annotation.lifecycle.OnEnabled
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.annotation.lifecycle.OnDisabled
import org.dcs.remote.ZkRemoteService
import org.dcs.remote.RemoteService
import org.apache.nifi.controller.AbstractControllerService
import scala.reflect.ClassTag

@Tags(Array("discovery", "zookeeper"))
@CapabilityDescription("Provides the ability to discover remote services via zookeeper")
class ZookeeperDiscoveryService extends AbstractControllerService with DiscoveryService {
  
  val Servers = new PropertyDescriptor
			.Builder().name("Zookeeper Servers")
			.description("The (space separated) list of zookeeper servers in '<domain>:<port>' form")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("")
			.build();
  
  val Properties = List[PropertyDescriptor](Servers)
  
  var remoteService: RemoteService = _
  

	override def  init(config: ControllerServiceInitializationContext) {
	  if(remoteService == null) remoteService = ZkRemoteService
	  
	}

	/**
	 * @param context
	 *            the configuration context
	 * @throws InitializationException
	 *             if unable to create a database connection
	 */
	@OnEnabled
	def onEnabled(context: ConfigurationContext)  {}

	@OnDisabled
	def shutdown() = remoteService.dispose
	

	override def service[T](implicit tag: ClassTag[T]): Option[T] = remoteService.loadService[T]
	
}