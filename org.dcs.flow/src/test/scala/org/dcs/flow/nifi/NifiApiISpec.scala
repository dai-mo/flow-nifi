package org.dcs.flow.nifi


import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.client.ProcessorApiSpec
import org.dcs.flow.ProcessorApi
import java.nio.file.Paths
import org.slf4j.LoggerFactory
import org.scalatest.FlatSpec
import org.slf4j.Logger
import org.dcs.flow.IT
import org.dcs.flow.BaseRestApi
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature

  object NifiApiISpec {
    object NifiApi extends BaseRestApi with NifiApiConfig
  }

  class NifiApiISpec extends RestBaseUnitSpec with NifiApiBehaviors {

    import NifiApiISpec._
    
    "Instantiating Templates" must " work correctly " taggedAs(IT) in {
      val tpath = "/controller/templates"
      val templates = NifiApi.getAsJson(tpath)

    }

  }

  trait NifiApiBehaviors { this: FlatSpec =>

    val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorApiSpec])
}