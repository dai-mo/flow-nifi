package org.dcs.flow.client

import java.io.File
import java.nio.file.{Path, Paths}

import org.dcs.flow.RestBaseUnitSpec
import org.dcs.flow.nifi.{NifiApiConfig, NifiProcessorApi}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.slf4j.{Logger, LoggerFactory}




object ProcessorApiSpec {
  class NifiProcessorClient extends ProcessorClient 
    with NifiProcessorApi 
    with NifiApiConfig 
}

class ProcessorApiSpec extends RestBaseUnitSpec with ProcessorApiBehaviors {

  import ProcessorApiSpec._


  val typesPath: Path = Paths.get(this.getClass().getResource("types.json").toURI())
  val processorClient = Mockito.spy(new NifiProcessorClient())
  doReturn(jsonFromFile(typesPath.toFile)).
    when(processorClient).
    getAsJson(NifiProcessorApi.TypesPath, Map(), Map())
  
  "Processor Types" must " be valid " in {
    validateProcessorTypes(processorClient)
  }
}

trait ProcessorApiBehaviors { this: FlatSpec =>
  import ProcessorApiSpec._

  val logger: Logger = LoggerFactory.getLogger(classOf[ProcessorApiSpec])

  def validateProcessorTypes(processorClient: NifiProcessorClient) {
      val types = processorClient.types()
      assert(types.size == 135)
  }
}