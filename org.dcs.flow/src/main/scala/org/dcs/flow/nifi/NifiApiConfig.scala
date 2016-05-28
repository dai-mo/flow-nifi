package org.dcs.flow.nifi

import org.dcs.commons.config.ConfigurationFacade
import org.dcs.flow.ApiConfig

object NifiApiConfig {
  val BaseUrl = ConfigurationFacade.config.nifiBaseUrl  
}

trait NifiApiConfig extends ApiConfig {
  import NifiApiConfig._
  
  override def baseUrl():String = BaseUrl
}